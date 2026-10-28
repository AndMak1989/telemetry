// Local stand-in for the OTel Lambda layer. Imported first by dev-server.ts and queue-runner.ts, so it runs
// before the app modules and can patch http, mongodb and the AWS SDK.
//
//   NEW_RELIC_LICENSE_KEY   ingest license key (type INGEST - LICENSE, not the browser NRJS key). Unset = tracing off.
//   OTEL_EXPORTER_OTLP_ENDPOINT   https://otlp.nr-data.net (US, default) or https://otlp.eu01.nr-data.net (EU)
//   LOCAL_OTEL=console      print spans to stdout instead of exporting (checks the wiring without an account)
//
// The SDK is imported dynamically so a run without tracing pays no startup cost. Top-level await keeps
// the import order: this module finishes before the app modules that import it start.
const key = process.env.NEW_RELIC_LICENSE_KEY;
const toConsole = process.env.LOCAL_OTEL === 'console';

if (key || toConsole) {
  // Traces only. Without these the SDK defaults metrics and logs to an OTLP collector on localhost:4318.
  process.env.OTEL_METRICS_EXPORTER ??= 'none';
  process.env.OTEL_LOGS_EXPORTER ??= 'none';

  const [{ NodeSDK }, { ConsoleSpanExporter }, { OTLPTraceExporter }, { HttpInstrumentation }, { MongoDBInstrumentation }, { AwsInstrumentation }] =
    await Promise.all([
      import('@opentelemetry/sdk-node'),
      import('@opentelemetry/sdk-trace-base'),
      import('@opentelemetry/exporter-trace-otlp-http'),
      import('@opentelemetry/instrumentation-http'),
      import('@opentelemetry/instrumentation-mongodb'),
      import('@opentelemetry/instrumentation-aws-sdk'),
    ]);

  const script = process.argv[1] ?? '';
  const serviceName =
    process.env.OTEL_SERVICE_NAME ??
    (script.includes('dev-server') ? 'myapp-api' : `myapp-${process.argv[2] ?? 'local'}`);
  const endpoint = (process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? 'https://otlp.nr-data.net').replace(/\/$/, '');

  const sdk = new NodeSDK({
    serviceName,
    traceExporter: toConsole
      ? new ConsoleSpanExporter()
      : new OTLPTraceExporter({ url: `${endpoint}/v1/traces`, headers: { 'api-key': key! } }),
    instrumentations: [
      new HttpInstrumentation(),
      new MongoDBInstrumentation(), // statement values are not recorded (enhancedDatabaseReporting off)
      new AwsInstrumentation(),
    ],
  });
  sdk.start();
  console.log(`▶ OTel tracing on: ${serviceName} → ${toConsole ? 'console' : endpoint}`);

  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.once(signal, () => void sdk.shutdown().finally(() => process.exit(0)));
  }
}

export {};
