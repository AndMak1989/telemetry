import { describe, expect, it } from 'vitest';
import { ObjectId } from 'mongodb';
import { SendMessageCommand } from '@aws-sdk/client-sqs';
import { handler } from '../src/functions/outbox-sweeper';
import { fakeContext, useAwsMocks, useTestDb } from './helpers';

const getDb = useTestDb();
const aws = useAwsMocks();

const outbox = async () => (await getDb()).collection('outbox');
const minutesAgo = (m: number) => new Date(Date.now() - m * 60_000);
const sweep = () => handler({}, fakeContext('outbox-sweeper', 60_000));

const sqsMessage = (orderId: string) => ({
  kind: 'sqs', queue: 'orders', body: { type: 'order.created', version: 1, orderId }, correlationId: 'req-1',
});
const eventMessage = (orderId: string) => ({
  kind: 'event', source: 'myapp.orders', detailType: 'OrderConfirmed', detail: { version: 1, orderId },
});

async function seedRow(message: object, { createdAt = minutesAgo(5), status = 'pending', attempts = 0 } = {}) {
  const _id = new ObjectId();
  await (await outbox()).insertOne({ _id, message, status, attempts, createdAt });
  return _id;
}
const row = async (_id: ObjectId) => (await outbox()).findOne({ _id });

describe('outbox-sweeper', () => {
  it('publishes stale pending rows of both kinds and marks them sent', async () => {
    const sqsRow = await seedRow(sqsMessage('o-1'));
    const eventRow = await seedRow(eventMessage('o-2'));

    await sweep();

    expect(aws.sentMessages()).toEqual([
      expect.objectContaining({
        QueueUrl: process.env.ORDERS_QUEUE_URL,
        MessageBody: JSON.stringify(sqsMessage('o-1').body),
        MessageAttributes: { correlationId: { DataType: 'String', StringValue: 'req-1' } },
      }),
    ]);
    expect(aws.putEvents()).toEqual([
      expect.objectContaining({ Source: 'myapp.orders', DetailType: 'OrderConfirmed', Detail: JSON.stringify(eventMessage('o-2').detail) }),
    ]);
    expect(await row(sqsRow)).toMatchObject({ status: 'sent', sentAt: expect.any(Date) });
    expect(await row(eventRow)).toMatchObject({ status: 'sent', sentAt: expect.any(Date) });
  });

  it('leaves rows younger than a minute to the inline publish, and skips finished rows', async () => {
    const fresh = await seedRow(sqsMessage('o-1'), { createdAt: new Date() });
    await seedRow(sqsMessage('o-2'), { status: 'sent' });
    await seedRow(sqsMessage('o-3'), { status: 'failed', attempts: 10 });

    await sweep();

    expect(aws.sentMessages()).toHaveLength(0);
    expect(await row(fresh)).toMatchObject({ status: 'pending', attempts: 0 });
  });

  it('publishes oldest first', async () => {
    await seedRow(sqsMessage('newer'), { createdAt: minutesAgo(2) });
    await seedRow(sqsMessage('older'), { createdAt: minutesAgo(30) });

    await sweep();

    expect(aws.sentMessages().map((m) => JSON.parse(m.MessageBody!).orderId)).toEqual(['older', 'newer']);
  });

  it('counts a failed attempt and keeps the row pending, without failing the invocation', async () => {
    aws.sqs.on(SendMessageCommand).rejects(new Error('throttled'));
    const id = await seedRow(sqsMessage('o-1'), { attempts: 3 });

    await expect(sweep()).resolves.toBeUndefined(); // retried next tick; the OutboxRetrying alarm watches this

    expect(await row(id)).toMatchObject({ status: 'pending', attempts: 4, lastError: expect.stringContaining('throttled') });
  });

  it('gives up after the last attempt, marks the row failed and fails the invocation', async () => {
    aws.sqs.on(SendMessageCommand).rejects(new Error('AccessDenied'));
    const doomed = await seedRow(sqsMessage('o-1'), { attempts: 9 });
    const retrying = await seedRow(sqsMessage('o-2'), { attempts: 0 });

    await expect(sweep()).rejects.toThrow('1 outbox rows marked failed'); // raises the SweeperErrors alarm

    expect(await row(doomed)).toMatchObject({ status: 'failed', attempts: 10 });
    expect(await row(retrying)).toMatchObject({ status: 'pending', attempts: 1 });
  });

  it('keeps publishing the rest of the batch after one row fails', async () => {
    aws.sqs.on(SendMessageCommand).rejectsOnce(new Error('blip')).resolves({ MessageId: 'ok' });
    const first = await seedRow(sqsMessage('o-1'), { createdAt: minutesAgo(10) });
    const second = await seedRow(sqsMessage('o-2'), { createdAt: minutesAgo(5) });

    await sweep();

    expect(await row(first)).toMatchObject({ status: 'pending', attempts: 1 });
    expect(await row(second)).toMatchObject({ status: 'sent' });
  });
});
