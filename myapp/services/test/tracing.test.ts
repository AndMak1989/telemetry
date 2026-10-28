import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { ObjectId } from 'mongodb';
import { SpanKind, trace } from '@opentelemetry/api';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import type { OrderCreatedMessage } from '@myapp/shared';
import { handler as ordersWorker } from '../src/functions/orders-worker';
import { handler as notificationsWorker } from '../src/functions/notifications-worker';
import { eventBridgeEnvelope, fakeContext, sqsEvent, useAwsMocks, useTestDb } from './helpers';

// A real SDK (W3C propagator, async context), as the Lambda layer or local/otel.ts would register it.
process.env.OTEL_METRICS_EXPORTER = 'none';
process.env.OTEL_LOGS_EXPORTER = 'none';
const exporter = new InMemorySpanExporter();
const sdk = new NodeSDK({ spanProcessors: [new SimpleSpanProcessor(exporter)], instrumentations: [] });
sdk.start();
afterAll(() => sdk.shutdown());

const getDb = useTestDb();
const aws = useAwsMocks();
beforeEach(() => exporter.reset());

async function seedPendingOrder() {
  const _id = new ObjectId();
  const now = new Date();
  await (await getDb()).collection('orders').insertOne({
    _id, customerId: 'c-1', items: [{ sku: 'A-1', qty: 1 }], status: 'pending', createdBy: 'user-1', createdAt: now, updatedAt: now, version: 1,
  });
  return _id.toHexString();
}
const created = (orderId: string): OrderCreatedMessage => ({
  type: 'order.created', version: 1, orderId, occurredAt: new Date().toISOString(),
});

describe('trace context across EventBridge', () => {
  it('the notification joins the trace in which the order was confirmed', async () => {
    const orderId = await seedPendingOrder();

    // Stands in for the Lambda SQS trigger span the worker runs under.
    const traceId = await trace.getTracer('test').startActiveSpan('orders-worker process', async (span) => {
      await ordersWorker(sqsEvent(created(orderId)), fakeContext('orders-worker'));
      span.end();
      return span.spanContext().traceId;
    });

    const detail = JSON.parse(aws.putEvents()[0]!.Detail!);
    expect(detail.traceparent).toMatch(new RegExp(`^00-${traceId}-[0-9a-f]{16}-01$`));

    // EventBridge → SQS carries no trace headers: only what is in the event body arrives.
    await notificationsWorker(sqsEvent(eventBridgeEnvelope('OrderConfirmed', detail)), fakeContext('notifications-worker'));

    const notify = exporter.getFinishedSpans().find((s) => s.name === 'notify order-confirmed');
    expect(notify).toBeDefined();
    expect(notify!.spanContext().traceId).toBe(traceId);
    expect(notify!.kind).toBe(SpanKind.CONSUMER);
  });

  it('an event without a traceparent (published before this change) is still processed', async () => {
    const detail = { version: 1, orderId: 'o-legacy', confirmedAt: new Date().toISOString() };

    const out = await notificationsWorker(sqsEvent(eventBridgeEnvelope('OrderConfirmed', detail)), fakeContext('notifications-worker'));

    expect(out.batchItemFailures).toEqual([]);
    expect(await (await getDb()).collection('notifications').countDocuments({ orderId: 'o-legacy' })).toBe(1);
    expect(exporter.getFinishedSpans().map((s) => s.name)).not.toContain('notify order-confirmed');
  });
});
