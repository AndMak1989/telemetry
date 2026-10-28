import { Logger } from '@aws-lambda-powertools/logger';
import { context, propagation, ROOT_CONTEXT, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api';

// The log level comes from Lambda's advanced logging controls (AWS_LAMBDA_LOG_LEVEL).
export const logger = new Logger({ serviceName: process.env.POWERTOOLS_SERVICE_NAME ?? 'myapp' });
const tracer = trace.getTracer('myapp'); // no-op unless an OTel SDK (the Lambda layer, or local/otel.ts) is running

/** `trace.id` / `span.id` are the attribute names New Relic uses for log-trace correlation. */
export function traceFields(): Record<string, string> {
  const ctx = trace.getActiveSpan()?.spanContext();
  return ctx ? { 'trace.id': ctx.traceId, 'span.id': ctx.spanId } : {};
}

/**
 * The active trace as a W3C `traceparent`, to carry across hops that do not propagate it themselves
 * (EventBridge → SQS drops it). Undefined when tracing is off.
 */
export function currentTraceparent(): string | undefined {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return carrier.traceparent;
}

/** Runs `fn` in a span that is a child of the trace in `traceparent`. Without one, just runs `fn`. */
export function withParentTrace<T>(traceparent: string | undefined, spanName: string, fn: () => Promise<T>): Promise<T> {
  if (!traceparent) return fn();
  const parent = propagation.extract(ROOT_CONTEXT, { traceparent });
  return tracer.startActiveSpan(spanName, { kind: SpanKind.CONSUMER }, parent, async (span) => {
    try {
      return await fn();
    } catch (err) {
      span.recordException(err as Error);
      span.setStatus({ code: SpanStatusCode.ERROR });
      throw err;
    } finally {
      span.end();
    }
  });
}
