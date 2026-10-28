import { describe, expect, it } from 'vitest';
import { ObjectId } from 'mongodb';
import { PutEventsCommand } from '@aws-sdk/client-eventbridge';
import { OrderConfirmedDetail, type OrderCreatedMessage } from '@myapp/shared';
import { handler } from '../src/functions/orders-worker';
import type { OrderDoc } from '../src/domain/orders';
import { fakeContext, sqsEvent, useAwsMocks, useTestDb } from './helpers';

const getDb = useTestDb();
const aws = useAwsMocks();

const orders = async () => (await getDb()).collection<OrderDoc>('orders');

async function seedOrder(status: OrderDoc['status'] = 'pending') {
  const now = new Date();
  const _id = new ObjectId();
  await (await orders()).insertOne({
    _id, customerId: 'c-1', items: [{ sku: 'A-1', qty: 1 }], status, createdBy: 'user-1', createdAt: now, updatedAt: now, version: 1,
  });
  return _id.toHexString();
}

const created = (orderId: string): OrderCreatedMessage => ({
  type: 'order.created', version: 1, orderId, occurredAt: new Date().toISOString(),
});

describe('orders-worker', () => {
  it('confirms the order and publishes OrderConfirmed with the correlation id', async () => {
    const orderId = await seedOrder();

    const out = await handler(
      sqsEvent({ body: created(orderId), attributes: { correlationId: 'req-42' } }),
      fakeContext('orders-worker'),
    );

    expect(out.batchItemFailures).toEqual([]);
    expect(await (await orders()).findOne({ _id: new ObjectId(orderId) })).toMatchObject({ status: 'confirmed', version: 2 });

    const [event] = aws.putEvents();
    expect(aws.putEvents()).toHaveLength(1);
    expect(event).toMatchObject({ EventBusName: process.env.EVENT_BUS_NAME, Source: 'myapp.orders', DetailType: 'OrderConfirmed' });
    expect(OrderConfirmedDetail.parse(JSON.parse(event!.Detail!))).toMatchObject({ orderId, correlationId: 'req-42' });
  });

  it('leaves optional fields out of the event instead of sending null (no correlation id, tracing off)', async () => {
    const orderId = await seedOrder();

    await handler(sqsEvent(created(orderId)), fakeContext('orders-worker'));

    const detail = JSON.parse(aws.putEvents()[0]!.Detail!);
    expect(detail).not.toHaveProperty('correlationId');
    expect(detail).not.toHaveProperty('traceparent');
    expect(() => OrderConfirmedDetail.parse(detail)).not.toThrow(); // what notifications-worker will do with it
  });

  it('fails only the bad records of a batch', async () => {
    const orderId = await seedOrder();

    const out = await handler(
      sqsEvent(created(orderId), { not: 'an order message' }, 'not json at all', created('not-an-object-id')),
      fakeContext('orders-worker'),
    );

    expect(out.batchItemFailures).toEqual([{ itemIdentifier: 'm-1' }, { itemIdentifier: 'm-2' }, { itemIdentifier: 'm-3' }]);
    expect(await (await orders()).findOne({ _id: new ObjectId(orderId) })).toMatchObject({ status: 'confirmed' });
  });

  it('processes a redelivered message once, in the same batch or a later one', async () => {
    const orderId = await seedOrder();

    const first = await handler(sqsEvent(created(orderId), created(orderId)), fakeContext('orders-worker'));
    const later = await handler(sqsEvent(created(orderId)), fakeContext('orders-worker'));

    expect(first.batchItemFailures).toEqual([]);
    expect(later.batchItemFailures).toEqual([]);
    expect(aws.putEvents()).toHaveLength(1);
    expect(await (await orders()).findOne({ _id: new ObjectId(orderId) })).toMatchObject({ version: 2 });
  });

  it('does nothing for an order that is no longer pending', async () => {
    const orderId = await seedOrder('cancelled');

    const out = await handler(sqsEvent(created(orderId)), fakeContext('orders-worker'));

    expect(out.batchItemFailures).toEqual([]); // not retried: retrying can't change the outcome
    expect(aws.putEvents()).toHaveLength(0);
    expect(await (await orders()).findOne({ _id: new ObjectId(orderId) })).toMatchObject({ status: 'cancelled', version: 1 });
  });

  it('keeps the confirmation when EventBridge rejects the event, leaving it to the sweeper', async () => {
    aws.eventBridge.on(PutEventsCommand).resolves({ FailedEntryCount: 1, Entries: [{ ErrorCode: 'InternalFailure', ErrorMessage: 'try again' }] });
    const orderId = await seedOrder();

    const out = await handler(sqsEvent(created(orderId)), fakeContext('orders-worker'));

    expect(out.batchItemFailures).toEqual([]); // the state change is committed; the outbox owns delivery now
    expect(await (await orders()).findOne({ _id: new ObjectId(orderId) })).toMatchObject({ status: 'confirmed' });
    const [row] = await (await getDb()).collection('outbox').find().toArray();
    expect(row).toMatchObject({ status: 'pending', attempts: 1, message: { kind: 'event', detailType: 'OrderConfirmed' } });
    expect(row!.lastError).toContain('InternalFailure');
  });
});
