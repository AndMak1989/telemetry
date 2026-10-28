import { ObjectId, type ClientSession, type Db } from 'mongodb';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
import { requireEnv } from './config';
import { logger } from './observability';

const sqs = new SQSClient({});
const eventBridge = new EventBridgeClient({});

// Logical names → env vars, so outbox rows are portable between environments.
const QUEUES = { orders: 'ORDERS_QUEUE_URL' } as const;
// About 45 minutes of retries at a 5-minute sweep interval.
const MAX_ATTEMPTS = 10;

export type OutboxMessage =
  | { kind: 'sqs'; queue: keyof typeof QUEUES; body: unknown; correlationId?: string }
  | { kind: 'event'; source: string; detailType: string; detail: unknown };

interface OutboxDoc {
  _id: ObjectId;
  message: OutboxMessage;
  status: 'pending' | 'sent' | 'failed';
  attempts: number;
  createdAt: Date;
  sentAt?: Date;
  lastError?: string;
}

export async function addToOutbox(db: Db, session: ClientSession, message: OutboxMessage) {
  const _id = new ObjectId();
  await db
    .collection<OutboxDoc>('outbox')
    .insertOne({ _id, message, status: 'pending', attempts: 0, createdAt: new Date() }, { session });
  return _id;
}

async function publish(message: OutboxMessage): Promise<void> {
  if (message.kind === 'sqs') {
    await sqs.send(
      new SendMessageCommand({
        QueueUrl: requireEnv(QUEUES[message.queue]),
        MessageBody: JSON.stringify(message.body),
        MessageAttributes: message.correlationId
          ? { correlationId: { DataType: 'String', StringValue: message.correlationId } }
          : undefined,
      }),
    );
    return;
  }
  const res = await eventBridge.send(
    new PutEventsCommand({
      Entries: [
        {
          EventBusName: requireEnv('EVENT_BUS_NAME'),
          Source: message.source,
          DetailType: message.detailType,
          Detail: JSON.stringify(message.detail),
        },
      ],
    }),
  );
  if (res.FailedEntryCount) {
    throw new Error(`PutEvents failed: ${res.Entries?.[0]?.ErrorCode} ${res.Entries?.[0]?.ErrorMessage}`);
  }
}

/** Publish the given outbox rows, or the oldest stale pending rows when no ids are given. */
export async function flushOutbox(
  db: Db,
  ids?: ObjectId[],
): Promise<{ sent: number; failed: number; deadLettered: number }> {
  const outbox = db.collection<OutboxDoc>('outbox');
  const filter = ids
    ? { _id: { $in: ids }, status: 'pending' as const }
    : { status: 'pending' as const, createdAt: { $lt: new Date(Date.now() - 60_000) } };
  const docs = await outbox.find(filter).sort({ createdAt: 1 }).limit(100).toArray();

  let sent = 0;
  let failed = 0;
  let deadLettered = 0;
  for (const doc of docs) {
    try {
      await publish(doc.message);
      await outbox.updateOne(
        { _id: doc._id, status: 'pending' },
        { $set: { status: 'sent', sentAt: new Date() } },
      );
      sent++;
    } catch (err) {
      failed++;
      const giveUp = doc.attempts + 1 >= MAX_ATTEMPTS;
      await outbox.updateOne(
        { _id: doc._id, status: 'pending' },
        { $inc: { attempts: 1 }, $set: { lastError: String(err), ...(giveUp && { status: 'failed' as const }) } },
      );
      if (giveUp) {
        deadLettered++;
        logger.error('Outbox row marked failed; needs manual replay', { outboxId: String(doc._id), err });
      } else {
        logger.warn('Outbox publish failed; the sweeper will retry', { outboxId: String(doc._id), err });
      }
    }
  }
  return { sent, failed, deadLettered };
}
