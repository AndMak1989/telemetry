import type { Db } from 'mongodb';

export async function ensureIndexes(db: Db): Promise<void> {
  await db.collection('orders').createIndexes([
    { key: { createdBy: 1, _id: -1 }, name: 'createdBy_id' },   // "my orders", newest first
    { key: { status: 1, createdAt: -1 }, name: 'status_createdAt' },
  ]);
  await db.collection('outbox').createIndexes([
    { key: { status: 1, createdAt: 1 }, name: 'status_createdAt' },
    { key: { sentAt: 1 }, name: 'sentAt_ttl', expireAfterSeconds: 7 * 24 * 3600 },
  ]);
  await db.collection('processed_messages').createIndex(
    { processedAt: 1 },
    { name: 'processedAt_ttl', expireAfterSeconds: 14 * 24 * 3600 }, // ≥ SQS max retention
  );
  await db.collection('notifications').createIndex({ orderId: 1 }, { name: 'orderId' });
}
