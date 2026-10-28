import type { Context, SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';
import { BatchProcessor, EventType, processPartialResponse } from '@aws-lambda-powertools/batch';
import { ObjectId } from 'mongodb';
import { OrderCreatedMessage, type OrderConfirmedDetail } from '@myapp/shared';
import { getDb, warmMongo } from '../lib/mongo';
import { processOnce } from '../lib/idempotency';
import { addToOutbox, flushOutbox } from '../lib/outbox';
import { currentTraceparent, logger, traceFields } from '../lib/observability';
import type { OrderDoc } from '../domain/orders';

warmMongo();
const processor = new BatchProcessor(EventType.SQS);

async function recordHandler(record: SQSRecord): Promise<void> {
  const correlationId = record.messageAttributes.correlationId?.stringValue;
  // Throws on bad input → this record fails → retried → DLQ after maxReceiveCount.
  const msg = OrderCreatedMessage.parse(JSON.parse(record.body));
  const db = await getDb();
  let outboxId: ObjectId | undefined;

  const outcome = await processOnce(`order-created:${msg.orderId}`, async (session) => {
    const now = new Date();
    const res = await db.collection<OrderDoc>('orders').updateOne(
      { _id: new ObjectId(msg.orderId), status: 'pending' },
      { $set: { status: 'confirmed', updatedAt: now }, $inc: { version: 1 } },
      { session },
    );
    if (res.matchedCount === 0) {
      logger.warn('Order not pending; nothing to do', { orderId: msg.orderId, correlationId });
      return;
    }
    // traceparent is stored with the outbox row, so an event the sweeper publishes later still joins this trace.
    // Optional fields are left out rather than set to undefined: Mongo would store null, which consumers reject.
    const traceparent = currentTraceparent();
    const detail: OrderConfirmedDetail = {
      version: 1,
      orderId: msg.orderId,
      confirmedAt: now.toISOString(),
      ...(correlationId && { correlationId }),
      ...(traceparent && { traceparent }),
    };
    outboxId = await addToOutbox(db, session, {
      kind: 'event',
      source: 'myapp.orders',
      detailType: 'OrderConfirmed',
      detail,
    });
  });

  if (outboxId) await flushOutbox(db, [outboxId]);
  logger.info('Processed order.created', { ...traceFields(), orderId: msg.orderId, outcome, messageId: record.messageId, correlationId });
}

export const handler = async (event: SQSEvent, context: Context): Promise<SQSBatchResponse> => {
  logger.addContext(context);
  return processPartialResponse(event, recordHandler, processor, { context });
};
