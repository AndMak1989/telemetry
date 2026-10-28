import { App, Tags } from 'aws-cdk-lib';
import { AppStack } from '../lib/app-stack';
import { loadStageConfig } from '../lib/config';

const app = new App();
const cfg = loadStageConfig(app); // -c stage=dev (default) or -c stage=prod
const samLocal = app.node.tryGetContext('samLocal') === 'true';
if (samLocal && cfg.stage === 'prod') throw new Error('samLocal is for local fidelity runs; use it with stage=dev only');

const stack = new AppStack(app, `MyApp-${cfg.stage}`, {
  config: samLocal ? { ...cfg, otel: undefined } : cfg, // SAM fidelity runs never use the OTel layers
  // Prod pins the account (validated in config): credentials for another account make the deploy fail.
  env: { account: cfg.account ?? process.env.CDK_DEFAULT_ACCOUNT, region: cfg.region },
  terminationProtection: cfg.stage === 'prod',
});

// Cost allocation and ownership on every resource.
Tags.of(stack).add('app', 'myapp');
Tags.of(stack).add('stage', cfg.stage);
Tags.of(stack).add('managed-by', 'cdk');
