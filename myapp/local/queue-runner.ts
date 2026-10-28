import type { Message } from '@aws-sdk/client-sqs';
import type { Context, SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';
import { fakeContext } from './fake-context';

// OTel must start before anything that it patches (AWS SDK, mongodb) is loaded, so these are dynamic.
// A static import would not wait for otel's top-level await. (No-op without NEW_RELIC_LICENSE_KEY.)
await import('./otel');
const { DeleteMessageBatchCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SQSClient } = await import('@aws-sdk/client-sqs');
const { requireEnv } = await import('../services/src/lib/config');
const { context, propagation, SpanKind, SpanStatusCode, trace } = await import('@opentelemetry/api');
const tracer = trace.getTracer('local-queue-runner'); // no-op unless tracing is on

// One process per function, like Lambda: the handlers share module-level singletons
// (Powertools logger context), so running several in one process mixes their log context.
// Usage: queue-runner.ts <orders-worker|notifications-worker|outbox-sweeper>
// Queue URLs come from .env.generated (written by bootstrap.ts).
type Worker = (e: SQSEvent, c: Context) => Promise<SQSBatchResponse | void>;
type Binding = { queueUrl: string; fn: string; handler: Worker; batchSize: number };

const fn = process.argv[2];

const sqs = new SQSClient({});

const toRecord = (m: Message, arn: string): SQSRecord => ({
  messageId: m.MessageId!,
  receiptHandle: m.ReceiptHandle!,
  body: m.Body ?? '',
  attributes: {
    ApproximateReceiveCount: m.Attributes?.ApproximateReceiveCount ?? '1',
    SentTimestamp: m.Attributes?.SentTimestamp ?? String(Date.now()),
    SenderId: m.Attributes?.SenderId ?? 'local',
    ApproximateFirstReceiveTimestamp: m.Attributes?.ApproximateFirstReceiveTimestamp ?? String(Date.now()),
  },
  messageAttributes: Object.fromEntries(
    Object.entries(m.MessageAttributes ?? {}).map(([k, v]) => [k, { dataType: v.DataType ?? 'String', stringValue: v.StringValue }]),
  ),
  md5OfBody: m.MD5OfBody ?? '',
  eventSource: 'aws:sqs',
  eventSourceARN: arn,
  awsRegion: process.env.AWS_REGION ?? 'us-east-1',
});

/**
 * Lambda's SQS trigger starts a span per invocation; emulate it. The sender's trace context travels in the
 * message attributes (the AWS SDK instrumentation adds it on send), so the worker joins the API request's trace
 * and the handler's log lines get a trace.id. A batch takes its parent from the first message.
 */
function traced<T>(fnName: string, first: Message, run: () => Promise<T>): Promise<T> {
  const carrier = Object.fromEntries(Object.entries(first.MessageAttributes ?? {}).map(([k, v]) => [k, v.StringValue ?? '']));
  const parent = propagation.extract(context.active(), carrier);
  return tracer.startActiveSpan(`${fnName} process`, { kind: SpanKind.CONSUMER }, parent, async (span) => {
    try {
      return await run();
    } catch (err) {
      span.recordException(err as Error);
      span.setStatus({ code: SpanStatusCode.ERROR });
      throw err;
    } finally {
      span.end();
    }
  });
}

async function poll(b: Binding): Promise<never> {
  const QueueUrl = b.queueUrl;
  const { Attributes } = await sqs.send(new GetQueueAttributesCommand({ QueueUrl, AttributeNames: ['QueueArn'] }));
  console.log(`▶ ${b.fn} ← ${QueueUrl}`);
  for (;;) {
    const { Messages = [] } = await sqs.send(new ReceiveMessageCommand({
      QueueUrl, MaxNumberOfMessages: Math.min(b.batchSize, 10), WaitTimeSeconds: 20,
      MessageAttributeNames: ['All'], MessageSystemAttributeNames: ['All'],
    }));
    if (!Messages.length) continue;

    const event: SQSEvent = { Records: Messages.map((m) => toRecord(m, Attributes!.QueueArn!)) };
    let failed = new Set<string>();
    try {
      const out = await traced(b.fn, Messages[0]!, () => b.handler(event, fakeContext(b.fn)));
      failed = new Set((out?.batchItemFailures ?? []).map((f) => f.itemIdentifier));
    } catch (err) {
      console.error(`✖ ${b.fn}: whole batch failed`, err); // messages reappear after the visibility timeout
      continue;
    }
    const done = Messages.filter((m) => !failed.has(m.MessageId!));
    if (done.length) {
      await sqs.send(new DeleteMessageBatchCommand({
        QueueUrl, Entries: done.map((m, i) => ({ Id: String(i), ReceiptHandle: m.ReceiptHandle! })),
      }));
    }
    console.log(`✔ ${b.fn}: ${done.length} ok, ${failed.size} failed (will retry → DLQ)`);
  }
}

if (fn === 'orders-worker') {
  const { handler } = await import('../services/src/functions/orders-worker');
  await poll({ queueUrl: requireEnv('ORDERS_QUEUE_URL'), fn, handler, batchSize: 10 });
} else if (fn === 'notifications-worker') {
  const { handler } = await import('../services/src/functions/notifications-worker');
  await poll({ queueUrl: requireEnv('NOTIFICATIONS_QUEUE_URL'), fn, handler, batchSize: 10 });
} else if (fn === 'outbox-sweeper') {
  const { handler } = await import('../services/src/functions/outbox-sweeper');
  // Emulate the 5-minute schedule (every minute locally).
  console.log('▶ outbox-sweeper every 60s');
  setInterval(() => handler({}, fakeContext(fn, 60_000)).catch((e) => console.error('sweeper', e)), 60_000);
} else {
  console.error('usage: queue-runner.ts <orders-worker|notifications-worker|outbox-sweeper>');
  process.exit(1);
}
