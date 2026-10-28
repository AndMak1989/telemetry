import { describe, expect, it } from 'vitest';
import { ObjectId } from 'mongodb';
import { SendMessageCommand } from '@aws-sdk/client-sqs';
import { OrderCreatedMessage } from '@myapp/shared';
import { handler } from '../src/functions/orders-api';
import type { OrderDoc } from '../src/domain/orders';
import { apiEvent, fakeContext, useAwsMocks, useTestDb } from './helpers';

const getDb = useTestDb();
const aws = useAwsMocks();

const validOrder = { customerId: 'c-1', items: [{ sku: 'A-1', qty: 2 }] };
const call = async (...args: Parameters<typeof apiEvent>) => {
  const res = await handler(apiEvent(...args), fakeContext('orders-api'));
  return { status: res.statusCode, body: res.body ? JSON.parse(res.body) : undefined };
};
const orders = async () => (await getDb()).collection<OrderDoc>('orders');
const outboxRows = async () => (await getDb()).collection('outbox').find().toArray();

/** Inserts orders directly, oldest first, so `_id` order is creation order. */
async function seedOrders(count: number, createdBy = 'user-1') {
  const now = new Date();
  const docs = Array.from({ length: count }, (_, i) => ({
    _id: new ObjectId(),
    customerId: `c-${i}`,
    items: [{ sku: 'A-1', qty: 1 }],
    status: 'pending' as const,
    createdBy,
    createdAt: now,
    updatedAt: now,
    version: 1,
  }));
  await (await orders()).insertMany(docs);
  return docs.map((d) => d._id.toHexString());
}

describe('POST /orders', () => {
  it('stores a pending order, publishes order.created and marks the outbox row sent', async () => {
    const res = await call('POST /orders', { body: validOrder });

    expect(res.status).toBe(201);
    expect(res.body).toEqual({ id: expect.any(String), status: 'pending' });

    const stored = await (await orders()).findOne({ _id: new ObjectId(res.body.id) });
    expect(stored).toMatchObject({ ...validOrder, status: 'pending', createdBy: 'user-1', version: 1 });

    const [msg] = aws.sentMessages();
    expect(aws.sentMessages()).toHaveLength(1);
    expect(msg!.QueueUrl).toBe(process.env.ORDERS_QUEUE_URL);
    expect(OrderCreatedMessage.parse(JSON.parse(msg!.MessageBody!))).toMatchObject({ orderId: res.body.id });
    expect(msg!.MessageAttributes?.correlationId?.StringValue).toBe('req-1'); // the API Gateway request id

    const rows = await outboxRows();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ status: 'sent', attempts: 0 });
  });

  it('still returns 201 when SQS is down, leaving the outbox row for the sweeper', async () => {
    aws.sqs.on(SendMessageCommand).rejects(new Error('SQS unavailable'));

    const res = await call('POST /orders', { body: validOrder });

    expect(res.status).toBe(201);
    expect(await (await orders()).countDocuments()).toBe(1); // the order is committed regardless
    const [row] = await outboxRows();
    expect(row).toMatchObject({ status: 'pending', attempts: 1 });
    expect(row!.lastError).toContain('SQS unavailable');
  });

  it('accepts a base64-encoded body', async () => {
    const res = await call('POST /orders', { body: validOrder, base64: true });
    expect(res.status).toBe(201);
  });

  it('accepts permissions as a real array claim, not only the flattened string', async () => {
    const res = await call('POST /orders', { body: validOrder, permissions: ['write:orders'] });
    expect(res.status).toBe(201);
  });

  it.each([
    ['no permissions', { permissions: [] }],
    ['read only', { permissions: '[read:orders]' }],
  ])('rejects a user with %s with 403 and writes nothing', async (_, auth) => {
    const res = await call('POST /orders', { body: validOrder, ...auth });

    expect(res.status).toBe(403);
    expect(res.body.message).toBe('Missing permission: write:orders');
    expect(await (await orders()).countDocuments()).toBe(0);
    expect(aws.sentMessages()).toHaveLength(0);
  });

  it('rejects a token without a subject with 401', async () => {
    const res = await call('POST /orders', { body: validOrder, sub: null });
    expect(res.status).toBe(401);
    expect(await (await orders()).countDocuments()).toBe(0);
  });

  it('rejects a body that is not JSON with 400', async () => {
    const res = await call('POST /orders', { rawBody: '{not json' });
    expect(res).toEqual({ status: 400, body: { message: 'Body must be valid JSON' } });
  });

  it.each([
    ['missing customerId', { items: [{ sku: 'A-1', qty: 1 }] }],
    ['no items', { customerId: 'c-1', items: [] }],
    ['zero quantity', { customerId: 'c-1', items: [{ sku: 'A-1', qty: 0 }] }],
    ['fractional quantity', { customerId: 'c-1', items: [{ sku: 'A-1', qty: 1.5 }] }],
    ['empty body', undefined],
  ])('rejects %s with 400 and the validation issues', async (_, body) => {
    const res = await call('POST /orders', { body });

    expect(res.status).toBe(400);
    expect(res.body.message).toBe('Validation failed');
    expect(res.body.issues.length).toBeGreaterThan(0);
    expect(await (await orders()).countDocuments()).toBe(0);
  });
});

describe('GET /orders', () => {
  it("returns only the caller's orders, newest first, as `id` without items", async () => {
    const mine = await seedOrders(2);
    await seedOrders(2, 'someone-else');

    const res = await call('GET /orders');

    expect(res.status).toBe(200);
    expect(res.body.items.map((o: { id: string }) => o.id)).toEqual([...mine].reverse());
    for (const item of res.body.items) {
      expect(item).not.toHaveProperty('_id');
      expect(item).not.toHaveProperty('items');
    }
    expect(res.body.nextCursor).toBeNull();
  });

  it('pages with a cursor until the last page', async () => {
    const ids = (await seedOrders(3)).reverse(); // newest first

    const page1 = await call('GET /orders', { query: { limit: '2' } });
    expect(page1.body.items.map((o: { id: string }) => o.id)).toEqual(ids.slice(0, 2));
    expect(page1.body.nextCursor).toBe(ids[1]);

    const page2 = await call('GET /orders', { query: { limit: '2', cursor: page1.body.nextCursor } });
    expect(page2.body.items.map((o: { id: string }) => o.id)).toEqual(ids.slice(2));
    expect(page2.body.nextCursor).toBeNull();
  });

  it.each([
    ['0', 20], // Mongo would treat limit(0) as "no limit"
    ['-5', 20],
    ['abc', 20],
    ['2.5', 20],
    ['500', 50], // capped
  ])('treats limit=%s as %i', async (limit, expected) => {
    await seedOrders(55);
    const res = await call('GET /orders', { query: { limit } });
    expect(res.body.items).toHaveLength(expected);
  });

  it('rejects an invalid cursor with 400', async () => {
    const res = await call('GET /orders', { query: { cursor: 'not-an-id' } });
    expect(res).toEqual({ status: 400, body: { message: 'Invalid cursor' } });
  });

  it('requires read:orders', async () => {
    const res = await call('GET /orders', { permissions: '[write:orders]' });
    expect(res.status).toBe(403);
  });
});

describe('GET /orders/{id}', () => {
  it('returns one of my orders', async () => {
    const [id] = await seedOrders(1);
    const res = await call('GET /orders/{id}', { pathParameters: { id: id! } });

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ id, customerId: 'c-0', status: 'pending', items: [{ sku: 'A-1', qty: 1 }] });
  });

  it("answers 404 for another user's order, so ids can't be probed", async () => {
    const [id] = await seedOrders(1, 'someone-else');
    const res = await call('GET /orders/{id}', { pathParameters: { id: id! } });
    expect(res).toEqual({ status: 404, body: { message: 'Order not found' } });
  });

  it('answers 404 for an id that does not exist', async () => {
    const res = await call('GET /orders/{id}', { pathParameters: { id: new ObjectId().toHexString() } });
    expect(res.status).toBe(404);
  });

  it('rejects a malformed id with 400', async () => {
    const res = await call('GET /orders/{id}', { pathParameters: { id: 'xyz' } });
    expect(res).toEqual({ status: 400, body: { message: 'Invalid id' } });
  });
});

it('answers 404 for an unknown route', async () => {
  const res = await call('DELETE /orders/{id}');
  expect(res).toEqual({ status: 404, body: { message: 'Not found' } });
});
