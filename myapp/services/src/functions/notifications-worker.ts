import type { Context, SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';
import { BatchProcessor, EventType, processPartialResponse } from '@aws-lambda-powertools/batch';
import { EventBridgeEnvelope, OrderConfirmedDetail } from '@myapp/shared';
import { getDb, warmMongo } from '../lib/mongo';
import { processOnce } from '../lib/idempotency';
import { logger, traceFields, withParentTrace } from '../lib/observability';

warmMongo();
const processor = new BatchProcessor(EventType.SQS);

async function recordHandler(record: SQSRecord): Promise<void> {
  const envelope = EventBridgeEnvelope.parse(JSON.parse(record.body)); // the SQS body is the EventBridge event
  if (envelope['detail-type'] !== 'OrderConfirmed') return;             // ignore other types deliberately
  const detail = OrderConfirmedDetail.parse(envelope.detail);
  const db = await getDb();

  // EventBridge → SQS drops trace context, so the producer put a traceparent in the event: join that trace.
  await withParentTrace(detail.traceparent, 'notify order-confirmed', async () => {
    // A business key, because a republished event gets a new EventBridge id.
    await processOnce(`notify-order-confirmed:${detail.orderId}`, async (session) => {
      await db.collection('notifications').insertOne(
        { orderId: detail.orderId, kind: 'order-confirmed', createdAt: new Date() },
        { session },
      );
    });
    logger.info('Notification recorded', {
      ...traceFields(),
      orderId: detail.orderId,
      eventId: envelope.id,
      messageId: record.messageId,
      correlationId: detail.correlationId,
    });
  });
}

export const handler = async (event: SQSEvent, context: Context): Promise<SQSBatchResponse> => {
  logger.addContext(context);
  return processPartialResponse(event, recordHandler, processor, { context });
};
