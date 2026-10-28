import * as lambda from 'aws-cdk-lib/aws-lambda';
import type { BundlingOptions } from 'aws-cdk-lib/aws-lambda-nodejs';
import type { Construct } from 'constructs';

export interface OtelConfig {
  enabled: boolean;
  samplingRatio: number;
  nodejsLayerArn: string;
  collectorLayerArn: string;
  grafanaOtlpEndpoint: string;
  newRelicIntegration?: { awsAccountId: string; externalId: string };
  grafanaIntegration?: { awsAccountId: string; externalId: string };
  newRelicLogForwarderArn?: string;
}

/** Import the two public layers once per stack. */
export function otelLayers(scope: Construct, cfg: OtelConfig): lambda.ILayerVersion[] {
  return [
    lambda.LayerVersion.fromLayerVersionArn(scope, 'OtelNodeLayer', cfg.nodejsLayerArn),
    lambda.LayerVersion.fromLayerVersionArn(scope, 'OtelCollectorLayer', cfg.collectorLayerArn),
  ];
}

/**
 * Auto-instrumentation patches modules as they are require()'d at runtime.
 * Anything esbuild bundles into one file can't be patched, so ship `mongodb` as a real
 * node_module. The AWS SDK is already external (provided by the runtime).
 * Also copy the collector config into the package.
 */
export function otelBundling(): Partial<BundlingOptions> {
  return {
    nodeModules: ['mongodb'],
    commandHooks: {
      beforeBundling: () => [],
      beforeInstall: () => [],
      afterBundling: (inputDir: string, outputDir: string) => [
        `cp ${inputDir}/infra/otel/collector.yaml ${outputDir}/collector.yaml`,
      ],
    },
  };
}

export function otelEnvironment(cfg: OtelConfig, serviceName: string, stage: string): Record<string, string> {
  const nrKey = process.env.NEW_RELIC_LICENSE_KEY;
  const grafanaAuth = process.env.GRAFANA_OTLP_AUTH;
  if (!nrKey || !grafanaAuth) {
    throw new Error('otel.enabled is true: set NEW_RELIC_LICENSE_KEY and GRAFANA_OTLP_AUTH before cdk synth/deploy');
  }
  return {
    AWS_LAMBDA_EXEC_WRAPPER: '/opt/otel-handler',
    OPENTELEMETRY_COLLECTOR_CONFIG_URI: '/var/task/collector.yaml',
    OTEL_SERVICE_NAME: serviceName,
    OTEL_RESOURCE_ATTRIBUTES: `deployment.environment=${stage},service.namespace=myapp`,
    OTEL_TRACES_SAMPLER: 'parentbased_traceidratio',
    OTEL_TRACES_SAMPLER_ARG: String(cfg.samplingRatio),
    OTEL_METRICS_EXPORTER: 'none', // metrics come from CloudWatch (see §16.2)
    OTEL_LOGS_EXPORTER: 'none',    // logs go through CloudWatch
    NEW_RELIC_LICENSE_KEY: nrKey,
    GRAFANA_OTLP_ENDPOINT: cfg.grafanaOtlpEndpoint,
    GRAFANA_OTLP_AUTH: grafanaAuth,
  };
}
