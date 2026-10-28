import type { App } from 'aws-cdk-lib';
import { RetentionDays } from 'aws-cdk-lib/aws-logs';
import type { OtelConfig } from './otel';

export const STAGES = ['dev', 'prod'] as const;
export type Stage = (typeof STAGES)[number];

/** One stage's settings: the `dev` / `prod` block under `context` in cdk.json. */
export interface StageConfig {
  stage: Stage;
  /** Required for prod: CDK then refuses to deploy with credentials for any other account. */
  account?: string;
  region: string;
  auth0Domain: string;   // a separate Auth0 tenant per stage
  auth0Audience: string; // the Auth0 API identifier
  webOrigin: string;     // CORS origin of the web app
  alarmEmail: string;
  /** Max concurrent invocations per SQS-triggered worker. Size it to the Mongo tier (M0: 2). */
  workerMaxConcurrency: number;
  apiThrottle: { rateLimit: number; burstLimit: number };
  logRetentionDays: RetentionDays;
  otel?: OtelConfig;
}

const PLACEHOLDER = /<[^>]+>|you@company\.com/;

/** Reads and checks the stage config. Fails fast, so a bad prod config never reaches CloudFormation. */
export function loadStageConfig(app: App): StageConfig {
  const stage = app.node.tryGetContext('stage') ?? 'dev';
  if (!STAGES.includes(stage)) throw new Error(`Unknown stage "${stage}". Use -c stage=dev or -c stage=prod.`);
  const raw = app.node.tryGetContext(stage);
  if (!raw) throw new Error(`cdk.json has no "${stage}" block under "context".`);
  const cfg: StageConfig = { ...raw, stage };

  const problems: string[] = [];
  for (const key of ['region', 'auth0Domain', 'auth0Audience', 'webOrigin', 'alarmEmail'] as const) {
    if (typeof cfg[key] !== 'string' || !cfg[key]) problems.push(`${key} is missing`);
  }
  // 2 is the lowest maxConcurrency an SQS event source accepts.
  if (!Number.isInteger(cfg.workerMaxConcurrency) || cfg.workerMaxConcurrency < 2) {
    problems.push('workerMaxConcurrency must be an integer ≥ 2');
  }
  if (!(cfg.apiThrottle?.rateLimit > 0) || !(cfg.apiThrottle?.burstLimit > 0)) {
    problems.push('apiThrottle.rateLimit and apiThrottle.burstLimit are required');
  }
  if (!Object.values(RetentionDays).includes(cfg.logRetentionDays)) {
    problems.push(`logRetentionDays ${cfg.logRetentionDays} is not a CloudWatch retention value (e.g. 14, 30, 90, 365)`);
  }
  if (cfg.otel?.enabled && PLACEHOLDER.test(JSON.stringify(cfg.otel))) {
    problems.push('otel is enabled but still has placeholders (layer ARNs)');
  }

  const placeholders = Object.entries(cfg)
    .filter(([k, v]) => k !== 'otel' && k !== 'account' && typeof v === 'string' && PLACEHOLDER.test(v))
    .map(([k]) => k);
  if (stage === 'prod') {
    if (!/^\d{12}$/.test(cfg.account ?? '')) problems.push('account must be the 12-digit prod AWS account ID');
    if (!cfg.webOrigin?.startsWith('https://')) problems.push('webOrigin must be https');
    if (placeholders.length) problems.push(`placeholders left in: ${placeholders.join(', ')}`);
  } else if (placeholders.length) {
    console.warn(`[config] ${stage}: placeholders left in ${placeholders.join(', ')}; fix them before deploying`);
  }

  if (problems.length) throw new Error(`Invalid "${stage}" config in cdk.json:\n- ${problems.join('\n- ')}`);
  return cfg;
}
