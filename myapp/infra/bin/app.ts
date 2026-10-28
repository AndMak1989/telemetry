import { App } from 'aws-cdk-lib';
import { AppStack } from '../lib/app-stack';

const app = new App();
const stage = (app.node.tryGetContext('stage') ?? 'dev') as 'dev' | 'prod';
const cfg = app.node.tryGetContext(stage);
const samLocal = app.node.tryGetContext('samLocal') === 'true';

new AppStack(app, `MyApp-${stage}`, {
  stage,
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: cfg.region },
  auth0Domain: cfg.auth0Domain,
  auth0Audience: cfg.auth0Audience,
  webOrigin: cfg.webOrigin,
  alarmEmail: cfg.alarmEmail,
  otel: samLocal ? undefined : cfg.otel, // SAM fidelity runs never use the OTel layers
});
