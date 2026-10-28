import * as path from 'node:path';
import { CfnOutput, Duration, RemovalPolicy, Stack, type StackProps } from 'aws-cdk-lib';
import type { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction, type NodejsFunctionProps } from 'aws-cdk-lib/aws-lambda-nodejs';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as destinations from 'aws-cdk-lib/aws-logs-destinations';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import { SnsAction } from 'aws-cdk-lib/aws-cloudwatch-actions';
import { CorsHttpMethod, HttpApi, HttpMethod, type CfnStage } from 'aws-cdk-lib/aws-apigatewayv2';
import { HttpJwtAuthorizer } from 'aws-cdk-lib/aws-apigatewayv2-authorizers';
import { HttpLambdaIntegration } from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import { API_ROUTES } from '../../services/src/routes';
import { otelBundling, otelEnvironment, otelLayers } from './otel';
import type { StageConfig } from './config';

export interface AppStackProps extends StackProps {
  config: StageConfig;
}

export class AppStack extends Stack {
  constructor(scope: Construct, id: string, props: AppStackProps) {
    super(scope, id, props);
    const cfg = props.config;
    const prefix = `myapp-${cfg.stage}`;
    // Prod keeps logs and dead letters when a resource is replaced or the stack is deleted; dev cleans up.
    const keep = cfg.stage === 'prod' ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY;

    // ---------- Secrets (create the SecureString yourself; see §15) ----------
    const mongoUriParamName = `/myapp/${cfg.stage}/mongo-uri`;
    const mongoUriParam = ssm.StringParameter.fromSecureStringParameterAttributes(this, 'MongoUri', {
      parameterName: mongoUriParamName,
    });

    // ---------- Messaging ----------
    const queueWithDlq = (name: string, visibility: Duration) => {
      const dlq = new sqs.Queue(this, `${name}Dlq`, {
        queueName: `${prefix}-${name}-dlq`,
        retentionPeriod: Duration.days(14),
        encryption: sqs.QueueEncryption.SQS_MANAGED,
        enforceSSL: true,
        removalPolicy: keep,
      });
      const queue = new sqs.Queue(this, `${name}Queue`, {
        queueName: `${prefix}-${name}`,
        visibilityTimeout: visibility,
        retentionPeriod: Duration.days(4),
        receiveMessageWaitTime: Duration.seconds(20),
        deadLetterQueue: { queue: dlq, maxReceiveCount: 5 },
        encryption: sqs.QueueEncryption.SQS_MANAGED,
        enforceSSL: true,
      });
      return { queue, dlq };
    };

    const WORKER_TIMEOUT = Duration.seconds(30);
    const orders = queueWithDlq('orders', Duration.seconds(180));       // 6 × 30 s
    const notifications = queueWithDlq('notifications', Duration.seconds(180));

    const bus = new events.EventBus(this, 'Bus', { eventBusName: `${prefix}-bus` });
    bus.archive('BusArchive', {
      eventPattern: { source: events.Match.prefix('myapp.') },
      retention: Duration.days(30),
    });
    const eventTargetDlq = new sqs.Queue(this, 'EventTargetDlq', {
      queueName: `${prefix}-event-target-dlq`,
      retentionPeriod: Duration.days(14),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
      removalPolicy: keep,
    });

    new events.Rule(this, 'OrderConfirmedToNotifications', {
      eventBus: bus,
      eventPattern: { source: ['myapp.orders'], detailType: ['OrderConfirmed'] },
      targets: [
        new targets.SqsQueue(notifications.queue, {
          deadLetterQueue: eventTargetDlq,
          retryAttempts: 8,
          maxEventAge: Duration.hours(2),
        }),
      ],
    });

    // ---------- Functions ----------
    // `-c samLocal=true` (§13.10 only, never deploy it): SAM's --env-vars can only override
    // variables the template declares, so declare the local-only ones as placeholders.
    const samLocal = this.node.tryGetContext('samLocal') === 'true';
    const commonEnv: Record<string, string> = {
      MONGO_URI_PARAM: mongoUriParamName,
      MONGO_DB_NAME: 'myapp',
      ORDERS_QUEUE_URL: orders.queue.queueUrl,
      EVENT_BUS_NAME: bus.eventBusName,
      NODE_OPTIONS: '--enable-source-maps',
      ...(samLocal ? { MONGO_URI: 'set-by-sam-env', AWS_ENDPOINT_URL: 'set-by-sam-env' } : {}),
    };

    const otel = cfg.otel?.enabled ? cfg.otel : undefined;
    const layers = otel ? otelLayers(this, otel) : [];
    // sameEnvironment: true guarantees CDK adds the logs.amazonaws.com invoke permission that each
    // subscription filter needs; without it CloudFormation rejects the filter and rolls back.
    const nrForwarder = otel?.newRelicLogForwarderArn
      ? lambda.Function.fromFunctionAttributes(this, 'NewRelicLogForwarder', {
          functionArn: otel.newRelicLogForwarderArn,
          sameEnvironment: true, // the forwarder is in this account and region
        })
      : undefined;

    const fn = (name: string, entry: string, extra: Partial<NodejsFunctionProps> = {}) => {
      const logGroup = new logs.LogGroup(this, `${name}Logs`, {
        retention: cfg.logRetentionDays,
        removalPolicy: keep,
      });
      const f = new NodejsFunction(this, name, {
        entry: path.join(__dirname, '../../services/src/functions', entry),
        handler: 'handler',
        runtime: lambda.Runtime.NODEJS_24_X,
        architecture: lambda.Architecture.ARM_64,
        memorySize: otel ? 768 : 512,                                // the layers add memory use; measure it
        timeout: Duration.seconds(10),
        logGroup,
        loggingFormat: lambda.LoggingFormat.JSON,
        applicationLogLevelV2: lambda.ApplicationLogLevel.INFO,
        systemLogLevelV2: lambda.SystemLogLevel.WARN,
        tracing: otel ? lambda.Tracing.PASS_THROUGH : lambda.Tracing.ACTIVE,
        layers,
        bundling: {
          minify: true,
          sourceMap: true,
          // No `target`: NodejsFunction derives it from `runtime` (node24), so the two can't drift.
          // The runtime ships AWS SDK v3; the Mongo driver's optional native add-ons aren't used.
          externalModules: ['@aws-sdk/*', 'kerberos', 'snappy', '@mongodb-js/zstd', 'mongodb-client-encryption', 'gcp-metadata', 'socks', 'aws4'],
          ...(otel ? otelBundling() : {}),
        },
        ...extra,
        environment: {
          ...commonEnv,
          POWERTOOLS_SERVICE_NAME: name,
          ...(otel ? otelEnvironment(otel, name, cfg.stage) : {}),
          ...extra.environment,
        },
      });
      mongoUriParam.grantRead(f);

      // New Relic logs: forward this function's logs to the New Relic log-ingestion Lambda.
      if (nrForwarder) {
        new logs.SubscriptionFilter(this, `${name}ToNewRelic`, {
          logGroup,
          destination: new destinations.LambdaDestination(nrForwarder),
          filterPattern: logs.FilterPattern.allEvents(),
        });
      }
      return f;
    };

    const ordersApi = fn('orders-api', 'orders-api.ts', { environment: { MONGO_MAX_POOL_SIZE: '2' } });
    const ordersWorker = fn('orders-worker', 'orders-worker.ts', {
      timeout: WORKER_TIMEOUT,
      environment: { MONGO_MAX_POOL_SIZE: '3' },
    });
    const notificationsWorker = fn('notifications-worker', 'notifications-worker.ts', {
      timeout: WORKER_TIMEOUT,
      environment: { MONGO_MAX_POOL_SIZE: '3' },
    });
    const outboxSweeper = fn('outbox-sweeper', 'outbox-sweeper.ts', {
      timeout: Duration.seconds(60),
      retryAttempts: 0, // Lambda's async retries (default 2); the next tick is the retry
    });

    // Least-privilege grants
    orders.queue.grantSendMessages(ordersApi);
    orders.queue.grantSendMessages(outboxSweeper);
    bus.grantPutEventsTo(ordersWorker);
    bus.grantPutEventsTo(outboxSweeper);

    // SQS triggers
    const sqsSource = (queue: sqs.IQueue) =>
      new SqsEventSource(queue, {
        batchSize: 10,
        maxBatchingWindow: Duration.seconds(2),
        reportBatchItemFailures: true,
        // Protects Mongo: concurrency × MONGO_MAX_POOL_SIZE must stay well under the tier's connection limit.
        maxConcurrency: cfg.workerMaxConcurrency,
      });
    ordersWorker.addEventSource(sqsSource(orders.queue));
    notificationsWorker.addEventSource(sqsSource(notifications.queue));

    // Outbox sweeper every 5 minutes
    new events.Rule(this, 'OutboxSweepSchedule', {
      schedule: events.Schedule.rate(Duration.minutes(5)),
      // retryAttempts here only covers EventBridge failing to *invoke* Lambda, not the function throwing.
      targets: [new targets.LambdaFunction(outboxSweeper, { retryAttempts: 1 })],
    });

    // ---------- HTTP API ----------
    const authorizer = new HttpJwtAuthorizer('Auth0Authorizer', `https://${cfg.auth0Domain}/`, {
      jwtAudience: [cfg.auth0Audience],
    });
    const api = new HttpApi(this, 'Api', {
      apiName: `${prefix}-api`,
      defaultAuthorizer: authorizer,
      corsPreflight: {
        allowOrigins: [cfg.webOrigin],
        allowMethods: [CorsHttpMethod.GET, CorsHttpMethod.POST, CorsHttpMethod.PUT, CorsHttpMethod.DELETE],
        // trace headers let the browser agents link their requests to backend traces (§16.9)
        allowHeaders: ['authorization', 'content-type', 'traceparent', 'tracestate', 'newrelic'],
        maxAge: Duration.hours(1),
      },
    });
    const integrations = { 'orders-api': new HttpLambdaIntegration('OrdersApiIntegration', ordersApi) };
    for (const r of API_ROUTES) {
      api.addRoutes({ path: r.path, methods: [HttpMethod[r.method]], integration: integrations[r.fn] });
    }
    // Stage-wide throttling (cheap DoS protection)
    const stage = api.defaultStage?.node.defaultChild as CfnStage;
    stage.defaultRouteSettings = {
      throttlingRateLimit: cfg.apiThrottle.rateLimit,
      throttlingBurstLimit: cfg.apiThrottle.burstLimit,
    };
    // Access logs: one JSON line per request, including the ones the authorizer rejects before any Lambda runs.
    const accessLogs = new logs.LogGroup(this, 'ApiAccessLogs', { retention: cfg.logRetentionDays, removalPolicy: keep });
    stage.accessLogSettings = {
      destinationArn: accessLogs.logGroupArn,
      format: JSON.stringify({
        requestId: '$context.requestId',
        ip: '$context.identity.sourceIp',
        requestTime: '$context.requestTime',
        routeKey: '$context.routeKey',
        status: '$context.status',
        latencyMs: '$context.responseLatency',
        userSub: '$context.authorizer.claims.sub',
        authorizerError: '$context.authorizer.error',
        integrationError: '$context.integrationErrorMessage',
      }),
    };

    // ---------- Alarms (10 free) ----------
    const alarmTopic = new sns.Topic(this, 'Alarms');
    alarmTopic.addSubscription(new subs.EmailSubscription(cfg.alarmEmail));
    const alarm = (id: string, metric: cloudwatch.IMetric, threshold: number) =>
      new cloudwatch.Alarm(this, id, {
        metric,
        threshold,
        evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      }).addAlarmAction(new SnsAction(alarmTopic));

    alarm('OrdersDlqNotEmpty', orders.dlq.metricApproximateNumberOfMessagesVisible(), 1);
    alarm('NotificationsDlqNotEmpty', notifications.dlq.metricApproximateNumberOfMessagesVisible(), 1);
    alarm('EventTargetDlqNotEmpty', eventTargetDlq.metricApproximateNumberOfMessagesVisible(), 1);
    alarm('OrdersBacklogOld', orders.queue.metricApproximateAgeOfOldestMessage({ period: Duration.minutes(5) }), 600);
    alarm('ApiErrors', ordersApi.metricErrors({ period: Duration.minutes(5) }), 5);
    // 5xx that never reach the function's error count: integration timeouts, throttled Lambda.
    alarm('Api5xx', api.metricServerError({ period: Duration.minutes(5) }), 5);
    alarm('WorkerErrors', ordersWorker.metricErrors({ period: Duration.minutes(5) }), 5);
    alarm('NotificationsWorkerErrors', notificationsWorker.metricErrors({ period: Duration.minutes(5) }), 5);
    alarm('SweeperErrors', outboxSweeper.metricErrors({ period: Duration.minutes(15) }), 1);

    // Ongoing publish failures (e.g. IAM broken by a deploy) long before any row gives up.
    // One data point per sweep that had failures, taken from the sweeper's "Outbox sweep" log line.
    const outboxRetries = outboxSweeper.logGroup.addMetricFilter('OutboxPublishFailures', {
      metricNamespace: 'MyApp',
      metricName: `${prefix}-outbox-publish-failures`,
      filterPattern: logs.FilterPattern.all(
        logs.FilterPattern.stringValue('$.message', '=', 'Outbox sweep'),
        logs.FilterPattern.numberValue('$.failed', '>', 0),
      ),
      metricValue: '$.failed',
    });
    // SampleCount ≥ 2 in 15 min = at least two of the last three sweeps failed; a single blip stays quiet.
    alarm('OutboxRetrying', outboxRetries.metric({ statistic: 'SampleCount', period: Duration.minutes(15) }), 2);

    // ---------- Read-only roles for the observability vendors (comparison only) ----------
    const vendorRole = (roleId: string, i: { awsAccountId: string; externalId: string }, policy: string) =>
      new iam.Role(this, roleId, {
        roleName: `${prefix}-${roleId}`,
        assumedBy: new iam.AccountPrincipal(i.awsAccountId),
        externalIds: [i.externalId],
        managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName(policy)],
      });

    if (otel?.newRelicIntegration) {
      // New Relic's wizard asks for ReadOnlyAccess so it can discover resources; narrow it later if you keep New Relic.
      const r = vendorRole('newrelic-integration', otel.newRelicIntegration, 'ReadOnlyAccess');
      new CfnOutput(this, 'NewRelicRoleArn', { value: r.roleArn });
    }
    if (otel?.grafanaIntegration) {
      // Metrics + Logs Insights read access for the Grafana CloudWatch data source.
      const r = vendorRole('grafana-cloudwatch', otel.grafanaIntegration, 'CloudWatchReadOnlyAccess');
      new CfnOutput(this, 'GrafanaRoleArn', { value: r.roleArn });
    }

    new CfnOutput(this, 'ApiUrl', { value: api.apiEndpoint });
  }
}
