# Serverless App Guide: TypeScript · Lambda · SQS · EventBridge · MongoDB · Auth0 · React

A build guide for a small enterprise app on free tiers, with production-grade practices and a local environment that costs nothing.

> **Verified October 2026.** Limits and free tiers change, so re-check the linked sources before you rely on a number.

---

## Contents

1. [Architecture](#1-architecture)
2. [Free-tier reality check](#2-free-tier-reality-check)
3. [Project layout and tooling](#3-project-layout-and-tooling)
4. [MongoDB: connections and data access](#4-mongodb-connections-and-data-access)
5. [Lambda: features and best practices](#5-lambda-features-and-best-practices)
6. [SQS: features and best practices](#6-sqs-features-and-best-practices)
7. [EventBridge: when and how](#7-eventbridge-when-and-how)
8. [Reliability patterns: idempotency and outbox](#8-reliability-patterns-idempotency-and-outbox)
9. [Function code](#9-function-code)
10. [Auth0 setup (free plan)](#10-auth0-setup-free-plan)
11. [React frontend](#11-react-frontend)
12. [Infrastructure with AWS CDK](#12-infrastructure-with-aws-cdk)
13. [Local development environment ($0)](#13-local-development-environment-0)
14. [Testing strategy](#14-testing-strategy)
15. [Deployment and CI/CD](#15-deployment-and-cicd)
16. [Observability](#16-observability): CloudWatch baseline + New Relic vs Grafana Cloud comparison
17. [Security](#17-security)
18. [Cost guardrails](#18-cost-guardrails)
19. [Checklists](#19-checklists)
20. [Sources](#20-sources)

---

## 1. Architecture

```
 React SPA (Vite)  ──login──▶  Auth0 (Universal Login, PKCE)
      │  access token (JWT, audience = your API)
      ▼
 CloudFront + S3 (static hosting)
      │
      ▼
 API Gateway HTTP API ── JWT authorizer (validates Auth0 token, no code)
      │
      ▼
 Lambda: orders-api ──────────────▶ MongoDB Atlas
      │  (order + outbox row written in one transaction)
      │ publish (after commit)
      ▼
 SQS: orders ──▶ Lambda: orders-worker ──▶ MongoDB
      │ (DLQ)                  │ outbox → EventBridge
      ▼                        ▼
 orders-dlq           EventBridge bus ──rule──▶ SQS: notifications ──▶ Lambda: notifications-worker
                               │                       │ (DLQ)
                               └──archive (replay)     ▼
                                                notifications-dlq
 EventBridge schedule (every 5 min) ──▶ Lambda: outbox-sweeper (re-publishes anything stuck)
```

**Key design decisions and why**

| Decision | Reason |
|---|---|
| **HTTP API (not REST API)** | Cheaper and simpler, with a built-in JWT authorizer that validates Auth0 tokens without custom code. |
| **SQS between producers and every Mongo-writing worker** | SQS buffers spikes, and *maximum concurrency* protects Mongo from connection storms. |
| **EventBridge → SQS → Lambda (never EventBridge → Lambda directly)** | You get buffering, a DLQ you control, batch processing and backpressure. |
| **One function per bounded context** (`orders-api`, not one per route) | Fewer cold starts, fewer Mongo connection pools, and simpler config. |
| **Transactional outbox** | A DB write and its message can't get out of sync if the SQS or EventBridge call fails. |
| **Idempotent consumers** | SQS and EventBridge deliver *at least once*, so duplicates must be harmless. |
| **Native `mongodb` driver + Zod** | Lighter cold starts than Mongoose and explicit validation. Mongoose is fine too if you set `bufferCommands: false` and `autoIndex: false`. |
| **Lambda outside a VPC** | Atlas Free can't use private networking (see §2), and a NAT gateway would cost money. |

**When to use SQS vs EventBridge**

- **SQS directly:** a command for one consumer, like "process this order". It's point-to-point and has the strongest control over retries.
- **EventBridge:** a fact that several consumers may care about, like "order confirmed". It gives you fan-out, routing by content, archive and replay. Always target an SQS queue.

---

## 2. Free-tier reality check

| Service | What's free | Gotchas that matter for this stack |
|---|---|---|
| **AWS Lambda** | Always free: 1M requests and 400,000 GB-seconds per month | The INIT phase is billed on managed runtimes, so keep init lean. New accounts may start with a **low concurrency quota**; request an increase in Service Quotas before go-live. |
| **Amazon SQS** | Always free: 1M requests per month | An SQS trigger long-polls **even when the queue is empty**. That can add up to hundreds of thousands of requests per queue per month, so keep the number of triggered queues small. Each 64 KB chunk of a payload counts as one request. |
| **API Gateway HTTP API** | 12-month free tier for *older* accounts only. Accounts on the new (post-July 2025) Free plan get none. | About $1 per million requests after that. A free alternative is a Lambda Function URL with JWT verification in code (§10.5). |
| **EventBridge** | Events from AWS services are free | Custom events are billed per million. Cheap, but not in the always-free list. |
| **CloudWatch** | 5 GB logs, 10 custom metrics, 10 alarms | **Always set log retention**; the default is "never expire". |
| **SSM Parameter Store** | Standard parameters, including SecureString with the AWS-managed key | Use this for secrets. **Secrets Manager costs about $0.40 per secret per month.** |
| **CloudFront / S3** | Always-free allowances | Plenty for a small internal SPA. |
| **MongoDB Atlas Free (M0)** | 512 MB storage, **500 connections**, **100 ops/sec**, permanent | **No private endpoints, no VPC peering, no backups, no SLA.** You must allowlist `0.0.0.0/0` because Lambda IPs change (see §17). |
| **Auth0 Free** | Up to **25,000 MAU**, 1 custom domain, passwordless, unlimited social connections, 1 enterprise connection | A credit card is needed to verify a custom domain. Rate limits are lower and support is community only. Over 25k MAU, you must upgrade. |

> **Enterprise warning:** Atlas M0 is fine for dev and pilots, but it has **no backups and no SLA**. For production business data, budget for a paid tier. Private endpoints need a dedicated (M10+) cluster.

> **LocalStack is no longer a $0 option for commercial work.** Since March 2026 LocalStack requires an account and auth token, and its free Hobby tier is non-commercial only. This guide uses **Floci** (MIT-licensed, no account) for local SQS and EventBridge. See §13.

---

## 3. Project layout and tooling

**Tooling:** Node.js 24 LTS · pnpm workspaces · TypeScript (strict) · esbuild (via CDK) · tsx (run TS locally) · Vitest · AWS CDK v2 · Podman (Docker works too).

```
myapp/
├─ apps/
│  └─ web/                      # React + Vite SPA
├─ packages/
│  └─ shared/                   # Zod schemas + types shared by web and services
│     └─ src/index.ts
├─ services/
│  ├─ src/
│  │  ├─ functions/             # Lambda entry points (thin)
│  │  │  ├─ orders-api.ts
│  │  │  ├─ orders-worker.ts
│  │  │  ├─ notifications-worker.ts
│  │  │  └─ outbox-sweeper.ts
│  │  ├─ lib/
│  │  │  ├─ mongo.ts            # cached client + pool settings
│  │  │  ├─ config.ts           # env + SSM parameters
│  │  │  ├─ http.ts             # JSON responses, errors, body parsing
│  │  │  ├─ auth.ts             # claims + permission checks
│  │  │  ├─ idempotency.ts      # process-once with Mongo
│  │  │  ├─ outbox.ts           # transactional outbox
│  │  ├─ schema.ts           # index definitions (migrations + tests)
│  │  │  └─ observability.ts    # Powertools logger
│  │  ├─ domain/orders.ts       # types + repository functions
│  │  └─ routes.ts             # single source of truth for API routes
│  └─ test/
├─ infra/                       # CDK app (separate package, CommonJS)
│  ├─ bin/app.ts
│  └─ lib/app-stack.ts
├─ local/                       # $0 local environment
│  ├─ docker-compose.yml
│  ├─ bootstrap.ts              # creates queues, DLQs, bus, rules in Floci
│  ├─ dev-server.ts             # HTTP → API Gateway v2 events → handlers
│  ├─ queue-runner.ts           # emulates the SQS → Lambda trigger
│  ├─ fake-context.ts
│  ├─ local-auth.ts             # offline JWT signing for tests/dev
│  └─ make-token.ts
├─ scripts/migrate.ts           # runs ensureIndexes() against an environment
├─ events/                      # sample Lambda events
├─ .env.local                   # local settings (git-ignored)
└─ pnpm-workspace.yaml
```

**Key packages**

```bash
# shared (use the same zod version everywhere, so `instanceof ZodError` works across packages)
pnpm --filter @myapp/shared add zod

# services
pnpm --filter services add '@myapp/shared@workspace:*' mongodb zod @aws-sdk/client-sqs @aws-sdk/client-eventbridge @aws-sdk/client-ssm \
  @aws-lambda-powertools/logger @aws-lambda-powertools/batch @aws-lambda-powertools/parameters
pnpm --filter services add -D @types/aws-lambda vitest mongodb-memory-server aws-sdk-client-mock

# root (local tooling)
# local/ and scripts/ live at the root and import these directly (pnpm doesn't hoist them)
pnpm add -D -w tsx typescript jose concurrently mongodb @aws-sdk/client-sqs @aws-sdk/client-eventbridge \
  @types/aws-lambda @types/node

# infra
pnpm --filter infra add aws-cdk-lib constructs
pnpm --filter infra add -D aws-cdk tsx

# web
pnpm --filter web add '@myapp/shared@workspace:*' @auth0/auth0-react @tanstack/react-query
```

**`pnpm-workspace.yaml`**

```yaml
packages: ['apps/*', 'packages/*', 'services', 'infra']
```

**`packages/shared/package.json`** (no build step: esbuild, Vite, tsx and Vitest all read the TypeScript source directly)

```json
{
  "name": "@myapp/shared",
  "private": true,
  "type": "module",
  "exports": "./src/index.ts"
}
```

Each package has its own `tsconfig.json` that extends `tsconfig.base.json`; the root one covers `local/` and `scripts/`.

**`tsconfig.base.json`**

```json
{
  "compilerOptions": {
    "target": "ES2023",
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "types": ["node"]
  }
}
```

The root and `services` package.json files set `"type": "module"`. The `infra` package does not, so CDK can use `__dirname`.

**Shared schemas: `packages/shared/src/index.ts`**

```ts
import { z } from 'zod';

export const OrderItem = z.object({
  sku: z.string().min(1).max(64),
  qty: z.number().int().positive().max(1000),
});

export const CreateOrderInput = z.object({
  customerId: z.string().min(1).max(64),
  items: z.array(OrderItem).min(1).max(100),
});
export type CreateOrderInput = z.infer<typeof CreateOrderInput>;

// Messages are versioned so you can evolve them without breaking consumers.
export const OrderCreatedMessage = z.object({
  type: z.literal('order.created'),
  version: z.literal(1),
  orderId: z.string(),
  occurredAt: z.string().datetime(),
});
export type OrderCreatedMessage = z.infer<typeof OrderCreatedMessage>;

export const OrderConfirmedDetail = z.object({
  version: z.literal(1),
  orderId: z.string(),
  confirmedAt: z.string().datetime(),
  correlationId: z.string().optional(), // trace context doesn't cross EventBridge; this does (§16.8)
});
export type OrderConfirmedDetail = z.infer<typeof OrderConfirmedDetail>;

// The envelope EventBridge wraps around every event delivered to SQS.
export const EventBridgeEnvelope = z.object({
  id: z.string(),
  'detail-type': z.string(),
  source: z.string(),
  time: z.string(),
  detail: z.unknown(),
});
```

---

## 4. MongoDB: connections and data access

### 4.1 Rules for Mongo in Lambda

1. **Create the client once, at module scope**, and reuse it across invocations. **Never** call `client.close()` in a handler.
2. **Cache the connection promise, not just the client**, so concurrent first calls don't open two pools. Reset the promise on failure so the next invocation retries.
3. **Keep `maxPoolSize` small.** Each Lambda instance handles one invocation at a time.
   - API functions: `2`
   - Workers processing a batch in parallel: `2–5`
4. **Start connecting during init**, so the TLS handshake happens before the first request.
5. **Set timeouts shorter than the Lambda timeout** so a slow DB fails fast instead of hanging until Lambda kills the function.
6. **Don't create indexes at runtime.** Run a migration script instead (§4.4).

### 4.2 Connection math (do this before choosing concurrency)

Each warm Lambda instance holds about `maxPoolSize + 1 monitoring connection per replica-set node`. The driver detects Lambda and switches to lighter polling monitoring. A 3-node Atlas cluster therefore costs about `maxPoolSize + 3` connections per instance.

```
API:     peak concurrency 20 × (2 + 3)          = 100
Workers: 2 queues × maxConcurrency 3 × (3 + 3)  =  36
Local dev / Compass / scripts                    ≈  20
                                                 ─────
                                                 ≈ 156  of 500 (M0)  ✅ keep below ~70%
```

On M0, the **100 ops/sec limit** usually bites before connections do. That's why workers run with low `maxConcurrency` and use bulk writes.

### 4.3 `services/src/lib/mongo.ts`

```ts
import { MongoClient, type Db } from 'mongodb';
import { getMongoUri } from './config';

let clientPromise: Promise<MongoClient> | undefined;

export function getMongoClient(): Promise<MongoClient> {
  if (!clientPromise) {
    clientPromise = (async () => {
      const client = new MongoClient(await getMongoUri(), {
        maxPoolSize: Number(process.env.MONGO_MAX_POOL_SIZE ?? 2),
        minPoolSize: 0,
        maxIdleTimeMS: 60_000,           // drop sockets idle across freezes
        serverSelectionTimeoutMS: 5_000, // fail fast if the cluster is unreachable
        connectTimeoutMS: 5_000,
        socketTimeoutMS: 20_000,         // keep below the Lambda timeout
        retryWrites: true,
        retryReads: true,
        appName: process.env.AWS_LAMBDA_FUNCTION_NAME ?? 'myapp-local',
      });
      await client.connect();
      return client;
    })().catch((err) => {
      clientPromise = undefined; // let the next invocation retry
      throw err;
    });
  }
  return clientPromise;
}

export async function getDb(): Promise<Db> {
  return (await getMongoClient()).db(process.env.MONGO_DB_NAME);
}

/** Call at module scope in each function file to connect during INIT. */
export function warmMongo(): void {
  getMongoClient().catch(() => {
    /* the handler will retry and surface the error */
  });
}

/** Tests only. */
export async function closeMongo(): Promise<void> {
  if (clientPromise) {
    const c = await clientPromise.catch(() => undefined);
    clientPromise = undefined;
    await c?.close();
  }
}
```

### 4.4 `services/src/lib/config.ts`

```ts
import { getParameter } from '@aws-lambda-powertools/parameters/ssm';

export function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var ${name}`);
  return v;
}

/** Local: MONGO_URI. AWS: SecureString in SSM (cached by Powertools for 5 min). */
export async function getMongoUri(): Promise<string> {
  if (process.env.MONGO_URI) return process.env.MONGO_URI;
  const name = requireEnv('MONGO_URI_PARAM');
  const value = await getParameter(name, { decrypt: true, maxAge: 300 });
  if (!value) throw new Error(`SSM parameter ${name} is empty`);
  return value;
}
```

### 4.5 Data-access best practices

- **Indexes:** create an index for every query pattern. Check new queries with `explain('executionStats')` (look for `IXSCAN`, not `COLLSCAN`).
- **Projection:** return only the fields you need.
- **Pagination:** use range-based cursors (`_id < lastId`), not `skip()`.
- **Bound every query:** always set `limit` and add `maxTimeMS` to reads that could be slow.
- **Bulk writes:** use `bulkWrite([...], { ordered: false })` when a batch touches many documents.
- **Transactions:** use them only when several documents must change together, such as an order plus its outbox row. Keep them short.
- **Optimistic concurrency:** keep a `version` field and update with `{ _id, version }` in the filter.
- **Store the Auth0 `sub`** as the user ID. Don't copy profile data you don't need.
- **Optional:** add a `$jsonSchema` validator on important collections as a second line of defense.

**`services/src/lib/schema.ts`** (index definitions, shared by the migration script and the tests)

```ts
import type { Db } from 'mongodb';

export async function ensureIndexes(db: Db): Promise<void> {
  await db.collection('orders').createIndexes([
    { key: { createdBy: 1, _id: -1 }, name: 'createdBy_id' },   // "my orders", newest first
    { key: { status: 1, createdAt: -1 }, name: 'status_createdAt' },
  ]);
  await db.collection('outbox').createIndexes([
    { key: { status: 1, createdAt: 1 }, name: 'status_createdAt' },
    { key: { sentAt: 1 }, name: 'sentAt_ttl', expireAfterSeconds: 7 * 24 * 3600 },
  ]);
  await db.collection('processed_messages').createIndex(
    { processedAt: 1 },
    { name: 'processedAt_ttl', expireAfterSeconds: 14 * 24 * 3600 }, // ≥ SQS max retention
  );
  await db.collection('notifications').createIndex({ orderId: 1 }, { name: 'orderId' });
}
```

**`scripts/migrate.ts`** (run once per environment and after schema changes, before the first deploy)

```ts
import { MongoClient } from 'mongodb';
import { ensureIndexes } from '../services/src/lib/schema';

const client = new MongoClient(process.env.MONGO_URI!);
await client.connect();
await ensureIndexes(client.db(process.env.MONGO_DB_NAME));
console.log('Indexes ready');
await client.close();
```

Running this also creates the collections up front, so transactions never have to create a collection implicitly. Two concurrent transactions creating the same collection conflict with each other; the driver retries, but it's slower and noisier.

---

## 5. Lambda: features and best practices

### 5.1 Feature map

| Feature | Use here? | How and why |
|---|---|---|
| **Runtime `nodejs24.x`** | ✅ | Supported until April 2028. Node 20 is deprecated, and Node 26 is in preview only. |
| **arm64 (Graviton)** | ✅ | Cheaper per GB-second with the same code. |
| **esbuild bundling + minify** | ✅ | Smaller packages mean faster cold starts. The CDK `NodejsFunction` does this. |
| **Init outside the handler** | ✅ | SDK clients and the Mongo connection are created once per instance. |
| **Memory tuning** | ✅ | Start at 512 MB, since more memory means more CPU and faster TLS. Tune with AWS Lambda Power Tuning. |
| **Timeouts** | ✅ | API: 10 s (HTTP API's hard limit is 30 s). Workers: 30 s. |
| **JSON logs + log levels** | ✅ | Use the built-in advanced logging controls: `loggingFormat: JSON` and `applicationLogLevelV2`. |
| **Log retention** | ✅ | 14–30 days. Never leave it at the default. |
| **Environment variables** | ✅ | Config only. Secrets go in SSM SecureString. |
| **SQS triggers** | ✅ | §6 covers batch size, batching window, partial failures and max concurrency. |
| **Trigger filter criteria** | ⚠️ | Saves invocations, but **SQS messages that don't match are deleted**. Use a separate queue instead of filters when in doubt. |
| **Reserved concurrency** | ⚠️ | Caps a function to protect Mongo, but new accounts with low quotas can't reserve. Prefer SQS max concurrency. |
| **Provisioned concurrency** | ❌ (cost) | Removes cold starts but is billed per hour. |
| **SQS provisioned mode** | ❌ (cost) | Faster scaling and more throughput, for strict-latency, high-volume workloads only. It can't be combined with max concurrency. |
| **SnapStart** | ❌ | Not available for Node.js. |
| **Versions and aliases** | Optional | Use with CodeDeploy for canary deploys once you have real traffic. |
| **Async invocation config** | ✅ (scheduled jobs) | Lambda retries a failed async invoke twice by default. Decide explicitly: here the sweeper uses `retryAttempts: 0`, since the next tick is its retry and the `Errors` alarm is its failure signal. Add an on-failure destination for async jobs that have no natural next run. |
| **Function URLs** | Optional | A free alternative to API Gateway (§10.5). |
| **Layers** | ❌ for app code | Bundling is simpler and tree-shakes. Use layers only for large shared binaries. |
| **X-Ray active tracing** | ✅ | 100k traces per month free. Turn sampling down if you exceed it. |
| **Recursive loop detection** | Built in | Lambda stops runaway SQS → Lambda → same-SQS loops. Don't design such loops. |

### 5.2 Handler rules

- Keep handlers **thin**: parse, validate, call domain functions, return. Domain logic should be testable without Lambda.
- Use **`async` handlers only**, not callback style.
- **Validate every input** (API bodies, SQS bodies, EventBridge `detail`) with Zod at the edge.
- **Never swallow errors in workers.** Let a failed record fail so SQS retries it and eventually sends it to the DLQ.
- Use **structured logs** with correlation IDs, and never log tokens or full documents containing PII.
- Make **every write idempotent** (§8).

### 5.3 Cold-start checklist

- Bundle and minify. Keep heavy SDKs out of hot paths, and use per-service v3 clients (`@aws-sdk/client-sqs`), never the whole SDK.
- The CDK default marks `@aws-sdk/*` as external, so the runtime's built-in SDK is used. That's fine; bundle it yourself only if you need a pinned version.
- Connect to Mongo during init with `warmMongo()`.
- Use 512 MB or more on API functions.
- Use one function per bounded context, so fewer instances need warming.

---

## 6. SQS: features and best practices

### 6.1 Queue configuration

| Setting | Value here | Why |
|---|---|---|
| **Type** | Standard | Use FIFO only when order truly matters per entity (§6.4). |
| **Visibility timeout** | **6× the function timeout** (30 s → 180 s) | AWS guidance. It prevents a message reappearing while a slow batch is still running. Lambda rejects a trigger whose function timeout exceeds it. |
| **Receive wait time** | 20 s | Long polling cuts empty receives for any custom consumer. |
| **Retention** | 4 days (main), 14 days (DLQ) | DLQ retention should be the maximum so you have time to investigate. |
| **Redrive policy** | `maxReceiveCount: 5` | After 5 failed receives, the message moves to the DLQ. |
| **Encryption** | SSE-SQS | Free and on by default for new queues. Avoid customer-managed KMS keys, which cost money. |
| **`enforceSSL`** | true | Denies non-TLS access in the queue policy. |
| **Max message size** | 1 MiB | Send IDs and small payloads. For anything big, store it in S3 or Mongo and send a reference (the "claim check" pattern). |

### 6.2 Trigger configuration

| Setting | Value here | Why |
|---|---|---|
| `batchSize` | 10 | Batches above 10 need a batching window. The whole batch must fit in Lambda's 6 MB payload limit. |
| `maxBatchingWindow` | 1–5 s | Fewer, fuller invocations when traffic is low. |
| `reportBatchItemFailures` | **true** | Only failed records are retried. Without this, one bad record retries the whole batch. |
| `maxConcurrency` | **2–5** on M0 | The main safety valve that protects Mongo. The minimum is 2. |

### 6.3 Producer best practices

- Use **`SendMessageBatch`** (up to 10 entries) when sending several messages. It's cheaper and faster, but **check `Failed[]`** in the response, because a partial failure doesn't throw.
- Add **message attributes** for metadata, such as `correlationId` and `type`.
- **Version message bodies** (`type` plus `version` fields).
- **Delays:** use `DelaySeconds` per message (standard queues) for retry-later patterns, up to 15 minutes. Use EventBridge Scheduler for longer delays.
- **Fair queues:** if one tenant can flood a shared standard queue, set a message group ID per tenant so other tenants aren't delayed.

### 6.4 FIFO queues (only if you need ordering)

- Ordering is **per `MessageGroupId`** (for example, `orderId`). Lambda concurrency is capped by the number of active groups.
- Deduplication uses `MessageDeduplicationId` or content-based deduplication, within a 5-minute window.
- **Partial batch failures behave differently.** After the first failure, you must also report every *later* record in that group as failed. Otherwise you break ordering. Use Powertools' SQS FIFO partial processor, which handles this.

### 6.5 DLQ operations

- Alarm on DLQ **`ApproximateNumberOfMessagesVisible ≥ 1`**.
- Inspect messages in the console. Fix the bug, then **redrive** with the console's "Start DLQ redrive" (StartMessageMoveTask).
- Poison messages, such as schema-invalid ones, will land in the DLQ after `maxReceiveCount` attempts. That's intended. Log them clearly.

### 6.6 Monitoring

- **`ApproximateAgeOfOldestMessage`:** the best "are we keeping up?" signal. Alarm at around 5–10 minutes.
- `ApproximateNumberOfMessagesVisible` and the function's `Errors` and `Throttles` metrics.

---

## 7. EventBridge: when and how

- **Custom bus** per app (for example, `myapp-dev-bus`). Don't use the default bus for app events.
- **Event shape:** `source` = `myapp.orders`, `detail-type` = `OrderConfirmed`, `detail` = a small versioned payload with IDs (`{ version: 1, orderId }`).
- **Rules route to SQS**, with a **target DLQ** plus `retryAttempts` and `maxEventAge`. A target DLQ catches delivery failures, such as a missing queue permission.
- **`PutEvents` partial failures don't throw.** Always check `FailedEntryCount`.
- **Archive and replay:** archive your bus with a retention period, so you can replay events after fixing a consumer bug.
- **Schedules:** use EventBridge rules with `rate()` or `cron()` (shown in §12) or EventBridge Scheduler (time zones, one-off schedules, its own retries and DLQ).
- **Optional:** use EventBridge Pipes (SQS → filter or enrich → target) when you'd otherwise write a "forwarding" Lambda.

---

## 8. Reliability patterns: idempotency and outbox

### 8.1 Idempotent consumers

SQS and EventBridge can deliver the same message twice, and the outbox can publish twice. Each consumer records a **business key** (for example, `order-created:<orderId>`) in `processed_messages`, using a unique `_id`, **in the same transaction** as its writes. A duplicate fails on the unique key and is skipped.

Use a **business key, not the SQS `messageId`**: a producer retry creates a new `messageId` for the same logical event.

**`services/src/lib/idempotency.ts`**

```ts
import { MongoServerError, type ClientSession } from 'mongodb';
import { getMongoClient } from './mongo';

class DuplicateMessage extends Error {}

export async function processOnce(
  key: string,
  work: (session: ClientSession) => Promise<void>,
): Promise<'processed' | 'duplicate'> {
  const client = await getMongoClient();
  const db = client.db(process.env.MONGO_DB_NAME);
  const session = client.startSession();
  try {
    await session.withTransaction(async () => {
      try {
        await db
          .collection<{ _id: string; processedAt: Date }>('processed_messages')
          .insertOne({ _id: key, processedAt: new Date() }, { session });
      } catch (err) {
        if (err instanceof MongoServerError && err.code === 11000) throw new DuplicateMessage();
        throw err;
      }
      await work(session);
    });
    return 'processed';
  } catch (err) {
    if (err instanceof DuplicateMessage) return 'duplicate'; // the transaction was aborted, nothing written
    throw err;
  } finally {
    await session.endSession();
  }
}
```

### 8.2 Transactional outbox

If you write to Mongo and then call SQS, a crash between the two loses the message. The fix:

1. Write the business document **and** an `outbox` document in the same transaction.
2. After commit, try to publish immediately and mark the outbox row `sent`.
3. A scheduled **sweeper** re-publishes any row still `pending` after 60 s.
4. After `MAX_ATTEMPTS` failures a row becomes `failed` (the outbox's own DLQ), so one unpublishable row can't fail every sweep forever or block newer rows. The sweeper throws when that happens, which fires the `SweeperErrors` alarm. Fix the cause, then set the row back to `{ status: 'pending', attempts: 0 }`. Retries that are still in progress raise a separate, earlier alarm (`OutboxRetrying`, §12) when two of the last three sweeps had failures.

Consumers are idempotent, so an occasional double publish is harmless.

Rows are published one at a time on purpose. The common case is a single row flushed right after commit, and per-row calls keep the status and error bookkeeping simple. If the sweeper regularly finds many stale rows, switch it to `SendMessageBatch` / multi-entry `PutEvents` and handle `Failed[]` / `Entries[].ErrorCode` per row.

**`services/src/lib/outbox.ts`**

```ts
import { ObjectId, type ClientSession, type Db } from 'mongodb';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
import { requireEnv } from './config';
import { logger } from './observability';

const sqs = new SQSClient({});
const eventBridge = new EventBridgeClient({});

// Logical names → env vars, so outbox rows are portable between environments.
const QUEUES = { orders: 'ORDERS_QUEUE_URL' } as const;
// About 45 minutes of retries at a 5-minute sweep interval.
const MAX_ATTEMPTS = 10;

export type OutboxMessage =
  | { kind: 'sqs'; queue: keyof typeof QUEUES; body: unknown; correlationId?: string }
  | { kind: 'event'; source: string; detailType: string; detail: unknown };

interface OutboxDoc {
  _id: ObjectId;
  message: OutboxMessage;
  status: 'pending' | 'sent' | 'failed';
  attempts: number;
  createdAt: Date;
  sentAt?: Date;
  lastError?: string;
}

export async function addToOutbox(db: Db, session: ClientSession, message: OutboxMessage) {
  const _id = new ObjectId();
  await db
    .collection<OutboxDoc>('outbox')
    .insertOne({ _id, message, status: 'pending', attempts: 0, createdAt: new Date() }, { session });
  return _id;
}

async function publish(message: OutboxMessage): Promise<void> {
  if (message.kind === 'sqs') {
    await sqs.send(
      new SendMessageCommand({
        QueueUrl: requireEnv(QUEUES[message.queue]),
        MessageBody: JSON.stringify(message.body),
        MessageAttributes: message.correlationId
          ? { correlationId: { DataType: 'String', StringValue: message.correlationId } }
          : undefined,
      }),
    );
    return;
  }
  const res = await eventBridge.send(
    new PutEventsCommand({
      Entries: [
        {
          EventBusName: requireEnv('EVENT_BUS_NAME'),
          Source: message.source,
          DetailType: message.detailType,
          Detail: JSON.stringify(message.detail),
        },
      ],
    }),
  );
  if (res.FailedEntryCount) {
    throw new Error(`PutEvents failed: ${res.Entries?.[0]?.ErrorCode} ${res.Entries?.[0]?.ErrorMessage}`);
  }
}

/** Publish the given outbox rows, or the oldest stale pending rows when no ids are given. */
export async function flushOutbox(
  db: Db,
  ids?: ObjectId[],
): Promise<{ sent: number; failed: number; deadLettered: number }> {
  const outbox = db.collection<OutboxDoc>('outbox');
  const filter = ids
    ? { _id: { $in: ids }, status: 'pending' as const }
    : { status: 'pending' as const, createdAt: { $lt: new Date(Date.now() - 60_000) } };
  const docs = await outbox.find(filter).sort({ createdAt: 1 }).limit(100).toArray();

  let sent = 0;
  let failed = 0;
  let deadLettered = 0;
  for (const doc of docs) {
    try {
      await publish(doc.message);
      await outbox.updateOne(
        { _id: doc._id, status: 'pending' },
        { $set: { status: 'sent', sentAt: new Date() } },
      );
      sent++;
    } catch (err) {
      failed++;
      const giveUp = doc.attempts + 1 >= MAX_ATTEMPTS;
      await outbox.updateOne(
        { _id: doc._id, status: 'pending' },
        { $inc: { attempts: 1 }, $set: { lastError: String(err), ...(giveUp && { status: 'failed' as const }) } },
      );
      if (giveUp) {
        deadLettered++;
        logger.error('Outbox row marked failed; needs manual replay', { outboxId: String(doc._id), err });
      } else {
        logger.warn('Outbox publish failed; the sweeper will retry', { outboxId: String(doc._id), err });
      }
    }
  }
  return { sent, failed, deadLettered };
}
```

---

## 9. Function code

### 9.1 Shared helpers

**`services/src/lib/observability.ts`**

```ts
import { Logger } from '@aws-lambda-powertools/logger';

// The log level comes from Lambda's advanced logging controls (AWS_LAMBDA_LOG_LEVEL).
export const logger = new Logger({ serviceName: process.env.POWERTOOLS_SERVICE_NAME ?? 'myapp' });
```

**`services/src/lib/http.ts`**

```ts
import type { APIGatewayProxyEventV2, APIGatewayProxyStructuredResultV2 } from 'aws-lambda';

export class HttpError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

export const json = (statusCode: number, body: unknown): APIGatewayProxyStructuredResultV2 => ({
  statusCode,
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(body),
});

export function parseBody(event: APIGatewayProxyEventV2): unknown {
  if (!event.body) return {};
  const raw = event.isBase64Encoded ? Buffer.from(event.body, 'base64').toString('utf8') : event.body;
  try {
    return JSON.parse(raw);
  } catch {
    throw new HttpError(400, 'Body must be valid JSON');
  }
}
```

**`services/src/lib/auth.ts`**

```ts
import type { APIGatewayProxyEventV2WithJWTAuthorizer } from 'aws-lambda';
import { HttpError } from './http';

export type ApiEvent = APIGatewayProxyEventV2WithJWTAuthorizer;

export function getUserId(event: ApiEvent): string {
  const sub = event.requestContext.authorizer?.jwt?.claims?.sub;
  if (typeof sub !== 'string' || !sub) throw new HttpError(401, 'Unauthorized');
  return sub;
}

/**
 * Auth0 RBAC puts permissions in a `permissions` array claim.
 * HTTP API's JWT authorizer flattens array claims into a string like "[read:orders write:orders]",
 * so handle both forms.
 */
export function getPermissions(event: ApiEvent): Set<string> {
  const raw = event.requestContext.authorizer?.jwt?.claims?.permissions;
  if (Array.isArray(raw)) return new Set(raw.map(String));
  if (typeof raw === 'string') {
    return new Set(raw.replace(/^\[|\]$/g, '').split(/[\s,]+/).filter(Boolean));
  }
  return new Set();
}

export function requirePermission(event: ApiEvent, permission: string): void {
  if (!getPermissions(event).has(permission)) throw new HttpError(403, `Missing permission: ${permission}`);
}
```

**`services/src/routes.ts`** (used by both the CDK and the local dev server)

```ts
export const API_ROUTES = [
  { method: 'GET', path: '/orders', fn: 'orders-api' },
  { method: 'POST', path: '/orders', fn: 'orders-api' },
  { method: 'GET', path: '/orders/{id}', fn: 'orders-api' },
] as const;

export type ApiFunctionName = (typeof API_ROUTES)[number]['fn'];
```

**`services/src/domain/orders.ts`**

```ts
import type { ObjectId } from 'mongodb';

export interface OrderDoc {
  _id: ObjectId;
  customerId: string;
  items: { sku: string; qty: number }[];
  status: 'pending' | 'confirmed' | 'shipped' | 'cancelled';
  createdBy: string; // Auth0 sub
  createdAt: Date;
  updatedAt: Date;
  version: number;
}
```

### 9.2 API function: `services/src/functions/orders-api.ts`

```ts
import type { APIGatewayProxyStructuredResultV2, Context } from 'aws-lambda';
import { ObjectId, type Filter } from 'mongodb';
import { ZodError } from 'zod';
import { CreateOrderInput, type OrderCreatedMessage } from '@myapp/shared';
import { getMongoClient, getDb, warmMongo } from '../lib/mongo';
import { addToOutbox, flushOutbox } from '../lib/outbox';
import { HttpError, json, parseBody } from '../lib/http';
import { getUserId, requirePermission, type ApiEvent } from '../lib/auth';
import { logger } from '../lib/observability';
import type { OrderDoc } from '../domain/orders';

warmMongo(); // connect during INIT

type Route = (event: ApiEvent) => Promise<APIGatewayProxyStructuredResultV2>;

// Clients see `id`, never Mongo's `_id`, matching what POST returns.
const toApi = ({ _id, ...rest }: Partial<OrderDoc> & Pick<OrderDoc, '_id'>) => ({ id: _id.toHexString(), ...rest });

const createOrder: Route = async (event) => {
  requirePermission(event, 'write:orders');
  const input = CreateOrderInput.parse(parseBody(event));
  const userId = getUserId(event);

  const client = await getMongoClient();
  const db = client.db(process.env.MONGO_DB_NAME);
  const orderId = new ObjectId();
  const now = new Date();
  const message: OrderCreatedMessage = {
    type: 'order.created',
    version: 1,
    orderId: orderId.toHexString(),
    occurredAt: now.toISOString(),
  };

  let outboxId: ObjectId | undefined;
  const session = client.startSession();
  try {
    await session.withTransaction(async () => {
      await db.collection<OrderDoc>('orders').insertOne(
        { _id: orderId, ...input, status: 'pending', createdBy: userId, createdAt: now, updatedAt: now, version: 1 },
        { session },
      );
      outboxId = await addToOutbox(db, session, {
        kind: 'sqs',
        queue: 'orders',
        body: message,
        correlationId: event.requestContext.requestId,
      });
    });
  } finally {
    await session.endSession();
  }

  // Best effort; the sweeper covers failures.
  if (outboxId) await flushOutbox(db, [outboxId]);

  return json(201, { id: orderId.toHexString(), status: 'pending' });
};

const listOrders: Route = async (event) => {
  requirePermission(event, 'read:orders');
  const db = await getDb();
  // Mongo treats limit(0) as "no limit", so reject 0, negatives and NaN.
  const requested = Number(event.queryStringParameters?.limit ?? 20);
  const limit = Number.isInteger(requested) && requested > 0 ? Math.min(requested, 50) : 20;
  const cursor = event.queryStringParameters?.cursor;

  const filter: Filter<OrderDoc> = { createdBy: getUserId(event) };
  if (cursor) {
    if (!ObjectId.isValid(cursor)) throw new HttpError(400, 'Invalid cursor');
    filter._id = { $lt: new ObjectId(cursor) };
  }

  const items = await db
    .collection<OrderDoc>('orders')
    .find(filter, { projection: { items: 0 } })
    .sort({ _id: -1 })
    .limit(limit)
    .maxTimeMS(3_000)
    .toArray();

  const last = items.at(-1);
  return json(200, {
    items: items.map(toApi),
    nextCursor: items.length === limit && last ? last._id.toHexString() : null,
  });
};

const getOrder: Route = async (event) => {
  requirePermission(event, 'read:orders');
  const id = event.pathParameters?.id;
  if (!id || !ObjectId.isValid(id)) throw new HttpError(400, 'Invalid id');
  const db = await getDb();
  const order = await db
    .collection<OrderDoc>('orders')
    .findOne({ _id: new ObjectId(id), createdBy: getUserId(event) }, { maxTimeMS: 3_000 });
  if (!order) throw new HttpError(404, 'Order not found');
  return json(200, toApi(order));
};

const routes: Record<string, Route> = {
  'POST /orders': createOrder,
  'GET /orders': listOrders,
  'GET /orders/{id}': getOrder,
};

export const handler = async (event: ApiEvent, context: Context): Promise<APIGatewayProxyStructuredResultV2> => {
  logger.addContext(context);
  const route = routes[event.routeKey];
  if (!route) return json(404, { message: 'Not found' });
  try {
    return await route(event);
  } catch (err) {
    if (err instanceof HttpError) return json(err.status, { message: err.message });
    if (err instanceof ZodError) return json(400, { message: 'Validation failed', issues: err.issues });
    logger.error('Unhandled error', { err, routeKey: event.routeKey, requestId: event.requestContext.requestId });
    return json(500, { message: 'Internal error' });
  }
};
```

### 9.3 SQS worker: `services/src/functions/orders-worker.ts`

```ts
import type { Context, SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';
import { BatchProcessor, EventType, processPartialResponse } from '@aws-lambda-powertools/batch';
import { ObjectId } from 'mongodb';
import { OrderCreatedMessage, type OrderConfirmedDetail } from '@myapp/shared';
import { getDb, warmMongo } from '../lib/mongo';
import { processOnce } from '../lib/idempotency';
import { addToOutbox, flushOutbox } from '../lib/outbox';
import { logger } from '../lib/observability';
import type { OrderDoc } from '../domain/orders';

warmMongo();
const processor = new BatchProcessor(EventType.SQS);

async function recordHandler(record: SQSRecord): Promise<void> {
  const correlationId = record.messageAttributes.correlationId?.stringValue;
  // Throws on bad input → this record fails → retried → DLQ after maxReceiveCount.
  const msg = OrderCreatedMessage.parse(JSON.parse(record.body));
  const db = await getDb();
  let outboxId: ObjectId | undefined;

  const outcome = await processOnce(`order-created:${msg.orderId}`, async (session) => {
    const now = new Date();
    const res = await db.collection<OrderDoc>('orders').updateOne(
      { _id: new ObjectId(msg.orderId), status: 'pending' },
      { $set: { status: 'confirmed', updatedAt: now }, $inc: { version: 1 } },
      { session },
    );
    if (res.matchedCount === 0) {
      logger.warn('Order not pending; nothing to do', { orderId: msg.orderId, correlationId });
      return;
    }
    const detail: OrderConfirmedDetail = { version: 1, orderId: msg.orderId, confirmedAt: now.toISOString(), correlationId };
    outboxId = await addToOutbox(db, session, {
      kind: 'event',
      source: 'myapp.orders',
      detailType: 'OrderConfirmed',
      detail,
    });
  });

  if (outboxId) await flushOutbox(db, [outboxId]);
  logger.info('Processed order.created', { orderId: msg.orderId, outcome, messageId: record.messageId, correlationId });
}

export const handler = async (event: SQSEvent, context: Context): Promise<SQSBatchResponse> => {
  logger.addContext(context);
  return processPartialResponse(event, recordHandler, processor, { context });
};
```

### 9.4 EventBridge-fed worker: `services/src/functions/notifications-worker.ts`

```ts
import type { Context, SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';
import { BatchProcessor, EventType, processPartialResponse } from '@aws-lambda-powertools/batch';
import { EventBridgeEnvelope, OrderConfirmedDetail } from '@myapp/shared';
import { getDb, warmMongo } from '../lib/mongo';
import { processOnce } from '../lib/idempotency';
import { logger } from '../lib/observability';

warmMongo();
const processor = new BatchProcessor(EventType.SQS);

async function recordHandler(record: SQSRecord): Promise<void> {
  const envelope = EventBridgeEnvelope.parse(JSON.parse(record.body)); // the SQS body is the EventBridge event
  if (envelope['detail-type'] !== 'OrderConfirmed') return;             // ignore other types deliberately
  const detail = OrderConfirmedDetail.parse(envelope.detail);
  const db = await getDb();

  // A business key, because a republished event gets a new EventBridge id.
  await processOnce(`notify-order-confirmed:${detail.orderId}`, async (session) => {
    await db.collection('notifications').insertOne(
      { orderId: detail.orderId, kind: 'order-confirmed', createdAt: new Date() },
      { session },
    );
  });
  logger.info('Notification recorded', {
    orderId: detail.orderId,
    eventId: envelope.id,
    messageId: record.messageId,
    correlationId: detail.correlationId,
  });
}

export const handler = async (event: SQSEvent, context: Context): Promise<SQSBatchResponse> => {
  logger.addContext(context);
  return processPartialResponse(event, recordHandler, processor, { context });
};
```

### 9.5 Scheduled sweeper: `services/src/functions/outbox-sweeper.ts`

```ts
import type { Context } from 'aws-lambda';
import { getDb, warmMongo } from '../lib/mongo';
import { flushOutbox } from '../lib/outbox';
import { logger } from '../lib/observability';

warmMongo();

export const handler = async (_event: unknown, context: Context): Promise<void> => {
  logger.addContext(context);
  const result = await flushOutbox(await getDb());
  if (result.sent || result.failed) logger.info('Outbox sweep', result);
  // Retriable failures are retried next tick; the `failed` count in the log above feeds the OutboxRetrying alarm.
  // Only rows that gave up raise the Errors metric.
  if (result.deadLettered) throw new Error(`${result.deadLettered} outbox rows marked failed`);
};
```

---

## 10. Auth0 setup (free plan)

### 10.1 Tenant

- Use **one tenant per environment** (`myapp-dev`, `myapp-prod`) if your account allows. Otherwise, use separate applications and API identifiers per environment.
- For an internal enterprise app, **disable public sign-ups**: *Authentication → Database → your connection → Disable Sign Ups*. Then invite users.
- Optionally add Google Workspace or Microsoft as a social connection, or use the free plan's single enterprise connection for company SSO.

### 10.2 API (resource server)

*Applications → APIs → Create API*

- **Identifier (audience):** for example `https://api.myapp.example`. It doesn't need to resolve.
- **Signing algorithm:** RS256.
- **Settings:**
  - Enable **RBAC** and **Add Permissions in the Access Token**.
  - Turn on **Allow Offline Access** (for refresh tokens).
  - Set **token expiration** to 3600 s (1 h) instead of the 24 h default.
- **Permissions:** `read:orders`, `write:orders` (add more as needed).
- **Roles** (*User Management → Roles*): `Viewer` (read), `Editor` (read + write). Assign roles to users.

### 10.3 SPA application

*Applications → Create → Single Page Application*

- **Allowed Callback URLs:** `http://localhost:5173, https://app.myapp.example`
- **Allowed Logout URLs** and **Allowed Web Origins:** the same.
- **Refresh Token Rotation:** on, with an absolute lifetime (for example, 7–30 days).

### 10.4 Validating tokens in AWS

The HTTP API **JWT authorizer** does the work:

- **Issuer:** `https://YOUR_TENANT.REGION.auth0.com/` (**the trailing slash matters**).
- **Audience:** your API identifier.

Lambda only runs for valid tokens. Handlers then check **permissions** (`requirePermission`) and **ownership** (`createdBy: sub`).

### 10.5 Free alternative: Function URL + verify in code

On a new AWS Free plan account with no API Gateway free tier, you can expose a Lambda Function URL (auth type `NONE`) and verify the JWT yourself with `jose`:

```ts
import { createRemoteJWKSet, jwtVerify } from 'jose';
const JWKS = createRemoteJWKSet(new URL(`https://${process.env.AUTH0_DOMAIN}/.well-known/jwks.json`)); // cached per instance
const { payload } = await jwtVerify(token, JWKS, {
  issuer: `https://${process.env.AUTH0_DOMAIN}/`,
  audience: process.env.AUTH0_AUDIENCE,
  algorithms: ['RS256'],
});
```

The trade-off: you lose built-in throttling, routing and CORS, and must handle them in code. For an enterprise app, the HTTP API's cost (about $1 per million requests) is usually worth it.

### 10.6 Free-plan notes

- **25,000 MAU limit.** Users count once per calendar month they log in.
- **Custom domain** (1 free, credit card verification needed). Recommended: it makes the SPA session survive reloads in browsers that block third-party cookies (§11).
- **Avoid machine-to-machine tokens** for AWS-internal calls; use IAM. If you need M2M, cache tokens until expiry.
- Auth0 log retention is short on the free plan, so stream important security events elsewhere if needed.

---

## 11. React frontend

**`apps/web/.env.development`**

```
VITE_AUTH0_DOMAIN=myapp-dev.us.auth0.com
VITE_AUTH0_CLIENT_ID=xxxxxxxx
VITE_AUTH0_AUDIENCE=https://api.myapp.example
VITE_API_URL=/api
```

**`apps/web/vite.config.ts`** (proxy to the local dev server, so there's no CORS in dev)

```ts
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { '/api': { target: 'http://localhost:3001', rewrite: (p) => p.replace(/^\/api/, '') } },
  },
});
```

**`apps/web/src/main.tsx`**

```tsx
import React from 'react';
import ReactDOM from 'react-dom/client';
import { Auth0Provider } from '@auth0/auth0-react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { App } from './App';

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 1, staleTime: 30_000 } } });

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <Auth0Provider
      domain={import.meta.env.VITE_AUTH0_DOMAIN}
      clientId={import.meta.env.VITE_AUTH0_CLIENT_ID}
      authorizationParams={{
        redirect_uri: window.location.origin,
        audience: import.meta.env.VITE_AUTH0_AUDIENCE,
        scope: 'openid profile email offline_access',
      }}
      useRefreshTokens
      cacheLocation="memory" // safest against XSS; see note below
    >
      <QueryClientProvider client={queryClient}>
        <App />
      </QueryClientProvider>
    </Auth0Provider>
  </React.StrictMode>,
);
```

> **Token storage:** with `cacheLocation="memory"`, a page reload relies on silent authentication via an iframe, which browsers that block third-party cookies break unless you use an **Auth0 custom domain** on the same site as your app. Your options are a custom domain (recommended) or `cacheLocation="localstorage"` plus a strict Content Security Policy.

**`apps/web/src/api.ts`**

```ts
import { useAuth0 } from '@auth0/auth0-react';
import { useCallback } from 'react';

export class ApiError extends Error {
  constructor(public status: number, message: string) { super(message); }
}

export function useApi() {
  const { getAccessTokenSilently } = useAuth0();
  return useCallback(
    async <T,>(path: string, init: RequestInit = {}): Promise<T> => {
      const token = await getAccessTokenSilently();
      const headers = new Headers(init.headers);
      headers.set('Authorization', `Bearer ${token}`);
      if (init.body) headers.set('Content-Type', 'application/json');
      const res = await fetch(`${import.meta.env.VITE_API_URL}${path}`, { ...init, headers });
      if (!res.ok) throw new ApiError(res.status, await res.text());
      return (res.status === 204 ? undefined : await res.json()) as T;
    },
    [getAccessTokenSilently],
  );
}
```

**Usage**

```tsx
const api = useApi();
const orders = useQuery({ queryKey: ['orders'], queryFn: () => api<{ items: Order[] }>('/orders') });
const create = useMutation({
  mutationFn: (input: CreateOrderInput) => api('/orders', { method: 'POST', body: JSON.stringify(input) }),
  onSuccess: () => queryClient.invalidateQueries({ queryKey: ['orders'] }),
});
```

- **Guard routes** with `withAuthenticationRequired(Component)`.
- **Hide UI by permission** for usability only. The API is the real enforcement.
- **Reuse validation:** import the Zod schemas from `@myapp/shared` and validate forms with the same rules as the API.

---

## 12. Infrastructure with AWS CDK

**`infra/lib/app-stack.ts`**

```ts
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
import * as ssm from 'aws-cdk-lib/aws-ssm';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import { SnsAction } from 'aws-cdk-lib/aws-cloudwatch-actions';
import { CorsHttpMethod, HttpApi, HttpMethod, type CfnStage } from 'aws-cdk-lib/aws-apigatewayv2';
import { HttpJwtAuthorizer } from 'aws-cdk-lib/aws-apigatewayv2-authorizers';
import { HttpLambdaIntegration } from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import { API_ROUTES } from '../../services/src/routes';

export interface AppStackProps extends StackProps {
  stage: 'dev' | 'prod';
  auth0Domain: string;   // myapp-dev.us.auth0.com
  auth0Audience: string; // https://api.myapp.example
  webOrigin: string;     // https://app.myapp.example
  alarmEmail: string;
}

export class AppStack extends Stack {
  constructor(scope: Construct, id: string, props: AppStackProps) {
    super(scope, id, props);
    const prefix = `myapp-${props.stage}`;

    // ---------- Secrets (create the SecureString yourself; see §15) ----------
    const mongoUriParamName = `/myapp/${props.stage}/mongo-uri`;
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

    const fn = (name: string, entry: string, extra: Partial<NodejsFunctionProps> = {}) => {
      const logGroup = new logs.LogGroup(this, `${name}Logs`, {
        retention: logs.RetentionDays.TWO_WEEKS,
        removalPolicy: RemovalPolicy.DESTROY,
      });
      const f = new NodejsFunction(this, name, {
        entry: path.join(__dirname, '../../services/src/functions', entry),
        handler: 'handler',
        runtime: lambda.Runtime.NODEJS_24_X, // use NODEJS_22_X if your CDK version predates Node 24
        architecture: lambda.Architecture.ARM_64,
        memorySize: 512,
        timeout: Duration.seconds(10),
        logGroup,
        loggingFormat: lambda.LoggingFormat.JSON,
        applicationLogLevelV2: lambda.ApplicationLogLevel.INFO,
        systemLogLevelV2: lambda.SystemLogLevel.WARN,
        tracing: lambda.Tracing.ACTIVE,
        bundling: {
          minify: true,
          sourceMap: true,
          // No `target`: NodejsFunction derives it from `runtime` (node24), so the two can't drift.
          // The runtime ships AWS SDK v3; the Mongo driver's optional native add-ons aren't used.
          externalModules: ['@aws-sdk/*', 'kerberos', 'snappy', '@mongodb-js/zstd', 'mongodb-client-encryption', 'gcp-metadata', 'socks', 'aws4'],
        },
        ...extra,
        environment: { ...commonEnv, POWERTOOLS_SERVICE_NAME: name, ...extra.environment },
      });
      mongoUriParam.grantRead(f);
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
        maxConcurrency: 2, // protects Mongo M0 (100 ops/s, 500 connections)
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
    const authorizer = new HttpJwtAuthorizer('Auth0Authorizer', `https://${props.auth0Domain}/`, {
      jwtAudience: [props.auth0Audience],
    });
    const api = new HttpApi(this, 'Api', {
      apiName: `${prefix}-api`,
      defaultAuthorizer: authorizer,
      corsPreflight: {
        allowOrigins: [props.webOrigin],
        allowMethods: [CorsHttpMethod.GET, CorsHttpMethod.POST, CorsHttpMethod.PUT, CorsHttpMethod.DELETE],
        allowHeaders: ['authorization', 'content-type'],
        maxAge: Duration.hours(1),
      },
    });
    const integrations = { 'orders-api': new HttpLambdaIntegration('OrdersApiIntegration', ordersApi) };
    for (const r of API_ROUTES) {
      api.addRoutes({ path: r.path, methods: [HttpMethod[r.method]], integration: integrations[r.fn] });
    }
    // Stage-wide throttling (cheap DoS protection)
    const stage = api.defaultStage?.node.defaultChild as CfnStage;
    stage.defaultRouteSettings = { throttlingRateLimit: 25, throttlingBurstLimit: 50 };

    // ---------- Alarms (10 free) ----------
    const alarmTopic = new sns.Topic(this, 'Alarms');
    alarmTopic.addSubscription(new subs.EmailSubscription(props.alarmEmail));
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

    new CfnOutput(this, 'ApiUrl', { value: api.apiEndpoint });
  }
}
```

**`infra/bin/app.ts`**

```ts
import { App } from 'aws-cdk-lib';
import { AppStack } from '../lib/app-stack';

const app = new App();
const stage = (app.node.tryGetContext('stage') ?? 'dev') as 'dev' | 'prod';
const cfg = app.node.tryGetContext(stage);

new AppStack(app, `MyApp-${stage}`, {
  stage,
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: cfg.region },
  auth0Domain: cfg.auth0Domain,
  auth0Audience: cfg.auth0Audience,
  webOrigin: cfg.webOrigin,
  alarmEmail: cfg.alarmEmail,
});
```

**`infra/cdk.json`**

```json
{
  "app": "npx tsx bin/app.ts",
  "context": {
    "dev": {
      "region": "us-east-1",
      "auth0Domain": "myapp-dev.us.auth0.com",
      "auth0Audience": "https://api.myapp.example",
      "webOrigin": "http://localhost:5173",
      "alarmEmail": "you@company.com"
    }
  }
}
```

**Web hosting (summary):** add a stack with a private S3 bucket and CloudFront using `S3BucketOrigin.withOriginAccessControl(bucket)`. Use `defaultRootObject: 'index.html'`, map 403/404 error responses to `/index.html` (SPA routing), add a response headers policy with a strict CSP, and deploy with `BucketDeployment` from `apps/web/dist`.

---

## 13. Local development environment ($0)

No AWS account, Atlas cluster or LocalStack token is needed. Auth0 can be real (a free dev tenant) or fully offline.

| Piece | Local replacement | Why |
|---|---|---|
| MongoDB Atlas | `mongo:8` in a Podman container as a **single-node replica set** | Transactions need a replica set. |
| SQS + EventBridge | **Floci** (MIT, no account) on port 4566 | Real SQS API, DLQs, buses, rules with SQS targets. |
| API Gateway + JWT authorizer | `local/dev-server.ts` | Builds real API Gateway v2 events, verifies JWTs and mimics claim flattening. |
| SQS → Lambda trigger | `local/queue-runner.ts` | Polls, batches, invokes and honors `batchItemFailures`. |
| Lambda runtime fidelity | **AWS SAM CLI** `sam local invoke` | Runs your *bundled* function in the official Lambda container image. |
| Unit and integration tests | Vitest + `mongodb-memory-server` + `aws-sdk-client-mock` | Fast and needs no container runtime. |

> Alternatives to Floci: **moto server** (`motoserver/moto`, MIT) also emulates SQS and EventBridge rules. **ElasticMQ** covers SQS only. Floci is newer, so pin an image version once things work.

### 13.1 `local/docker-compose.yml`

```yaml
services:
  mongo:
    image: mongo:8
    command: ["--replSet", "rs0", "--bind_ip_all", "--port", "27018"]
    ports: ["27018:27018"]
    volumes: ["mongo-data:/data/db"]
    healthcheck:
      # Initiates the replica set on first start, then reports healthy.
      test: ["CMD", "mongosh", "--quiet", "--port", "27018", "--eval",
             "try { rs.status().ok } catch (e) { rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: '127.0.0.1:27018' }] }).ok }"]
      interval: 5s
      timeout: 10s
      retries: 30
      start_period: 5s

  floci:
    image: floci/floci:2.3.0   # pinned; bump it on purpose
    ports: ["4566:4566"]
    environment:
      - FLOCI_DEFAULT_REGION=us-east-1
    volumes: ["./.floci:/app/data"]

volumes:
  mongo-data: {}
```

Connect from your machine with `mongodb://127.0.0.1:27018/?replicaSet=rs0`. The port is 27018, not 27017, so it never collides with a MongoDB already installed on your machine; the member is advertised as `127.0.0.1` because `localhost` can resolve to IPv6 on Windows/Podman, where the port forward does not listen. MongoDB Compass (free) works against it too.

### 13.2 `.env.local` (git-ignored)

```bash
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_ENDPOINT_URL=http://localhost:4566        # AWS SDK v3 sends every client here

MONGO_URI=mongodb://127.0.0.1:27018/?replicaSet=rs0
MONGO_DB_NAME=myapp

AUTH_MODE=auth0                                # or "local" for fully offline tokens
AUTH0_DOMAIN=myapp-dev.us.auth0.com
AUTH0_AUDIENCE=https://api.myapp.example
LOCAL_API_PORT=3001
```

`bootstrap.ts` writes `.env.generated` with the queue URLs and bus name.

### 13.3 `local/bootstrap.ts`: queues, DLQs, bus, rule

```ts
import { writeFileSync } from 'node:fs';
import { CreateQueueCommand, GetQueueAttributesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { CreateEventBusCommand, EventBridgeClient, PutRuleCommand, PutTargetsCommand } from '@aws-sdk/client-eventbridge';

const sqs = new SQSClient({});
const eb = new EventBridgeClient({});
const BUS = 'myapp-local-bus';

async function arnOf(url: string) {
  const r = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ['QueueArn'] }));
  return r.Attributes!.QueueArn!;
}

async function queueWithDlq(name: string) {
  const dlq = await sqs.send(new CreateQueueCommand({
    QueueName: `${name}-dlq`,
    Attributes: { MessageRetentionPeriod: String(14 * 24 * 3600) },
  }));
  const q = await sqs.send(new CreateQueueCommand({
    QueueName: name,
    Attributes: {
      VisibilityTimeout: '30',          // shorter than prod so local retries are quick
      ReceiveMessageWaitTimeSeconds: '20',
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: await arnOf(dlq.QueueUrl!), maxReceiveCount: 3 }),
    },
  }));
  return { url: q.QueueUrl!, arn: await arnOf(q.QueueUrl!) };
}

const orders = await queueWithDlq('orders');
const notifications = await queueWithDlq('notifications');

try {
  await eb.send(new CreateEventBusCommand({ Name: BUS }));
} catch (err) {
  if ((err as Error).name !== 'ResourceAlreadyExistsException') throw err;
}
await eb.send(new PutRuleCommand({
  Name: 'order-confirmed-to-notifications',
  EventBusName: BUS,
  EventPattern: JSON.stringify({ source: ['myapp.orders'], 'detail-type': ['OrderConfirmed'] }),
}));
await eb.send(new PutTargetsCommand({
  Rule: 'order-confirmed-to-notifications',
  EventBusName: BUS,
  Targets: [{ Id: 'notifications-queue', Arn: notifications.arn }],
}));

writeFileSync('.env.generated', [
  `ORDERS_QUEUE_URL=${orders.url}`,
  `NOTIFICATIONS_QUEUE_URL=${notifications.url}`,
  `EVENT_BUS_NAME=${BUS}`,
].join('\n') + '\n');
console.log('Local AWS resources ready → .env.generated');
```

### 13.4 `local/fake-context.ts`

```ts
import { randomUUID } from 'node:crypto';
import type { Context } from 'aws-lambda';

export function fakeContext(functionName: string, timeoutMs = 30_000): Context {
  const deadline = Date.now() + timeoutMs;
  return {
    callbackWaitsForEmptyEventLoop: false,
    functionName,
    functionVersion: '$LATEST',
    invokedFunctionArn: `arn:aws:lambda:us-east-1:000000000000:function:${functionName}`,
    memoryLimitInMB: '512',
    awsRequestId: randomUUID(),
    logGroupName: `/aws/lambda/${functionName}`,
    logStreamName: 'local',
    getRemainingTimeInMillis: () => Math.max(0, deadline - Date.now()),
    done: () => {},
    fail: () => {},
    succeed: () => {},
  };
}
```

### 13.5 `local/local-auth.ts` + `local/make-token.ts` (offline tokens)

```ts
// local/local-auth.ts
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { exportJWK, generateKeyPair, importJWK, type JWK } from 'jose';

const DIR = new URL('./.keys/', import.meta.url);
const FILE = new URL('dev-key.json', DIR);
export const LOCAL_ISSUER = 'https://local.dev/';

export async function loadKeys() {
  if (!existsSync(FILE)) {
    const { privateKey } = await generateKeyPair('RS256', { extractable: true });
    mkdirSync(DIR, { recursive: true });
    writeFileSync(FILE, JSON.stringify({ ...(await exportJWK(privateKey)), kid: 'local-dev', alg: 'RS256' }));
  }
  const priv = JSON.parse(readFileSync(FILE, 'utf8')) as JWK;
  const { d, p, q, dp, dq, qi, ...publicJwk } = priv; // strip private parts
  return { privateKey: await importJWK(priv, 'RS256'), publicJwk };
}
```

```ts
// local/make-token.ts   →   pnpm token --sub user-1 --perms read:orders,write:orders
import { parseArgs } from 'node:util';
import { SignJWT } from 'jose';
import { LOCAL_ISSUER, loadKeys } from './local-auth';

const { values } = parseArgs({ options: { sub: { type: 'string', default: 'local|user-1' }, perms: { type: 'string', default: 'read:orders,write:orders' } } });
const { privateKey } = await loadKeys();
const token = await new SignJWT({ permissions: values.perms!.split(','), scope: 'openid profile' })
  .setProtectedHeader({ alg: 'RS256', kid: 'local-dev' })
  .setIssuer(LOCAL_ISSUER)
  .setAudience(process.env.AUTH0_AUDIENCE!)
  .setSubject(values.sub!)
  .setIssuedAt()
  .setExpirationTime('8h')
  .sign(privateKey);
console.log(token);
```

### 13.6 `local/dev-server.ts`: API Gateway emulator

```ts
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { randomUUID } from 'node:crypto';
import { createLocalJWKSet, createRemoteJWKSet, jwtVerify, type JWTPayload } from 'jose';
import type { APIGatewayProxyEventV2WithJWTAuthorizer, APIGatewayProxyStructuredResultV2, Context } from 'aws-lambda';
import { API_ROUTES, type ApiFunctionName } from '../services/src/routes';
import { handler as ordersApi } from '../services/src/functions/orders-api';
import { LOCAL_ISSUER, loadKeys } from './local-auth';
import { fakeContext } from './fake-context';

type Handler = (e: APIGatewayProxyEventV2WithJWTAuthorizer, c: Context) => Promise<APIGatewayProxyStructuredResultV2>;
const HANDLERS: Record<ApiFunctionName, Handler> = { 'orders-api': ordersApi };

const local = process.env.AUTH_MODE === 'local';
const ISSUER = local ? LOCAL_ISSUER : `https://${process.env.AUTH0_DOMAIN}/`;
const JWKS = local
  ? createLocalJWKSet({ keys: [(await loadKeys()).publicJwk] })
  : createRemoteJWKSet(new URL(`${ISSUER}.well-known/jwks.json`));

const compiled = API_ROUTES.map((r) => {
  const keys: string[] = [];
  const re = new RegExp('^' + r.path.replace(/\{(\w+)\}/g, (_, k: string) => (keys.push(k), '([^/]+)')) + '$');
  return { ...r, re, keys };
});

// Mimic API Gateway: every claim becomes a string. Arrays → "[a b]", objects → JSON, the rest → String(v).
const flatten = (p: JWTPayload) =>
  Object.fromEntries(Object.entries(p).map(([k, v]) => [k, Array.isArray(v) ? `[${v.join(' ')}]` : v !== null && typeof v === 'object' ? JSON.stringify(v) : String(v)]));

const readBody = (req: IncomingMessage) =>
  new Promise<string>((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c)).on('end', () => resolve(Buffer.concat(chunks).toString('utf8'))).on('error', reject);
  });

const send = (res: ServerResponse, r: APIGatewayProxyStructuredResultV2) => {
  res.writeHead(r.statusCode ?? 200, { 'content-type': 'application/json', ...(r.headers as Record<string, string>) });
  res.end(r.body ?? '');
};

createServer(async (req, res) => {
  try {
    const method = req.method ?? 'GET';
    const url = new URL(req.url ?? '/', 'http://localhost');
    let params: Record<string, string> = {};
    const route = compiled.find((r) => {
      const m = r.method === method && r.re.exec(url.pathname);
      if (m) params = Object.fromEntries(r.keys.map((k, i) => [k, decodeURIComponent(m[i + 1]!)]));
      return Boolean(m);
    });
    if (!route) return send(res, { statusCode: 404, body: '{"message":"Not Found"}' });

    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    let payload: JWTPayload;
    try {
      ({ payload } = await jwtVerify(token ?? '', JWKS, { issuer: ISSUER, audience: process.env.AUTH0_AUDIENCE, algorithms: ['RS256'] }));
    } catch {
      return send(res, { statusCode: 401, body: '{"message":"Unauthorized"}' });
    }

    const body = await readBody(req);
    const routeKey = `${method} ${route.path}`;
    const event: APIGatewayProxyEventV2WithJWTAuthorizer = {
      version: '2.0',
      routeKey,
      rawPath: url.pathname,
      rawQueryString: url.searchParams.toString(),
      headers: Object.fromEntries(Object.entries(req.headers).map(([k, v]) => [k, Array.isArray(v) ? v.join(',') : v])),
      queryStringParameters: url.searchParams.size ? Object.fromEntries(url.searchParams) : undefined,
      pathParameters: Object.keys(params).length ? params : undefined,
      body: body || undefined,
      isBase64Encoded: false,
      requestContext: {
        accountId: '000000000000', apiId: 'local', domainName: 'localhost', domainPrefix: 'localhost',
        http: { method, path: url.pathname, protocol: 'HTTP/1.1', sourceIp: '127.0.0.1', userAgent: req.headers['user-agent'] ?? '' },
        requestId: randomUUID(), routeKey, stage: '$default', time: new Date().toISOString(), timeEpoch: Date.now(),
        authorizer: {
          principalId: '', integrationLatency: 0,
          jwt: { claims: flatten(payload), scopes: typeof payload.scope === 'string' ? payload.scope.split(' ') : [] },
        },
      },
    };
    send(res, await HANDLERS[route.fn](event, fakeContext(route.fn, 10_000)));
  } catch (err) {
    console.error(err);
    send(res, { statusCode: 500, body: '{"message":"dev-server error"}' });
  }
}).listen(Number(process.env.LOCAL_API_PORT ?? 3001), () => console.log('Local API on :3001'));
```

### 13.7 `local/queue-runner.ts`: SQS trigger emulator

```ts
import {
  DeleteMessageBatchCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SQSClient, type Message,
} from '@aws-sdk/client-sqs';
import type { Context, SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';
import { handler as ordersWorker } from '../services/src/functions/orders-worker';
import { handler as notificationsWorker } from '../services/src/functions/notifications-worker';
import { handler as outboxSweeper } from '../services/src/functions/outbox-sweeper';
import { requireEnv } from '../services/src/lib/config';
import { fakeContext } from './fake-context';

// Queue URLs come from .env.generated (written by bootstrap.ts).
type Worker = (e: SQSEvent, c: Context) => Promise<SQSBatchResponse | void>;
const bindings: { queueUrl: string; fn: string; handler: Worker; batchSize: number }[] = [
  { queueUrl: requireEnv('ORDERS_QUEUE_URL'), fn: 'orders-worker', handler: ordersWorker, batchSize: 10 },
  { queueUrl: requireEnv('NOTIFICATIONS_QUEUE_URL'), fn: 'notifications-worker', handler: notificationsWorker, batchSize: 10 },
];

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

async function poll(b: (typeof bindings)[number]): Promise<never> {
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
      const out = await b.handler(event, fakeContext(b.fn));
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

// Emulate the 5-minute schedule (every minute locally).
setInterval(() => outboxSweeper({}, fakeContext('outbox-sweeper', 60_000)).catch((e) => console.error('sweeper', e)), 60_000);
await Promise.all(bindings.map(poll));
```

### 13.8 Root `package.json` scripts

```json
{
  "type": "module",
  "scripts": {
    "local:up": "node -e \"require('fs').mkdirSync('local/.floci',{recursive:true})\" && podman compose -f local/docker-compose.yml up -d --wait",
    "local:down": "podman compose -f local/docker-compose.yml down",
    "local:reset": "podman compose -f local/docker-compose.yml down -v && node -e \"for(const p of ['local/.floci','local/.keys','.env.generated'])require('fs').rmSync(p,{recursive:true,force:true})\"",
    "local:bootstrap": "tsx --env-file=.env.local local/bootstrap.ts",
    "migrate": "tsx --env-file=.env.local scripts/migrate.ts",
    "token": "tsx --env-file=.env.local local/make-token.ts",
    "dev:api": "tsx watch --env-file=.env.local --env-file=.env.generated local/dev-server.ts",
    "dev:workers": "tsx watch --env-file=.env.local --env-file=.env.generated local/queue-runner.ts",
    "dev:web": "pnpm --filter web dev",
    "dev": "concurrently -n api,workers,web -c blue,magenta,green \"pnpm dev:api\" \"pnpm dev:workers\" \"pnpm dev:web\"",
    "test": "pnpm --filter services test",
    "typecheck": "tsc --noEmit && pnpm -r exec tsc --noEmit",
    "synth": "pnpm --filter infra exec cdk synth -c stage=dev",
    "synth:sam": "pnpm --filter infra exec cdk synth -c stage=dev -c samLocal=true"
  }
}
```

### 13.9 First run

```bash
pnpm install
pnpm local:up            # Mongo replica set + Floci
pnpm local:bootstrap     # queues, DLQs, bus, rule → .env.generated
pnpm migrate             # indexes + collections
pnpm dev                 # API :3001, workers, React :5173
```

**Smoke test without the UI (offline auth):**

`AUTH_MODE=local` is for curl and headless testing. The dev server then trusts only `https://local.dev/` tokens, so the SPA's Auth0 tokens get 401 until you switch back to `auth0`.

```bash
# In .env.local set AUTH_MODE=local, then restart `pnpm dev`
TOKEN=$(pnpm -s token --perms read:orders,write:orders)
curl -s -X POST localhost:3001/orders -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"customerId":"c-1","items":[{"sku":"A-1","qty":2}]}'
# Watch the worker logs: orders-worker confirms the order → EventBridge → notifications-worker
curl -s localhost:3001/orders -H "Authorization: Bearer $TOKEN"     # status should become "confirmed"
```

**Test failure handling:** send a malformed message straight to the queue, then watch it retry and land in `orders-dlq`:

```bash
set -a && source .env.local && source .env.generated && set +a
aws --endpoint-url http://localhost:4566 sqs send-message --queue-url "$ORDERS_QUEUE_URL" --message-body '{"bad":true}'
# After 3 failed receives (local maxReceiveCount), it appears in orders-dlq:
aws --endpoint-url http://localhost:4566 sqs receive-message --queue-url "${ORDERS_QUEUE_URL}-dlq"
```

**Debugging:** open a VS Code *JavaScript Debug Terminal* and run `pnpm dev:api` or `pnpm dev:workers`. Breakpoints in handler TypeScript work directly.

### 13.10 Fidelity check with SAM CLI (real Lambda container)

The dev server runs your TypeScript in plain Node. Before deploying, also run the **bundled artifact** in the official Lambda image:

```bash
pnpm synth:sam   # adds MONGO_URI / AWS_ENDPOINT_URL placeholders and skips OTel; never deploy this template
# Find the function's logical ID
jq -r '.Resources | to_entries[] | select(.value.Type=="AWS::Lambda::Function") | .key' infra/cdk.out/MyApp-dev.template.json

sam local generate-event sqs receive-message --body '{"type":"order.created","version":1,"orderId":"65f0c0ffee0000000000000a","occurredAt":"2026-10-27T10:00:00Z"}' > events/order-created.json

sam local invoke -t infra/cdk.out/MyApp-dev.template.json OrdersWorkerXXXXXXXX \
  -e events/order-created.json --env-vars local/sam-env.json --add-host host.docker.internal:host-gateway
```

**`local/sam-env.json`** (applies to all functions)

```json
{ "Parameters": {
  "MONGO_URI": "mongodb://host.docker.internal:27018/myapp?directConnection=true",
  "MONGO_DB_NAME": "myapp",
  "AWS_ENDPOINT_URL": "http://host.docker.internal:4566",
  "ORDERS_QUEUE_URL": "http://host.docker.internal:4566/000000000000/orders",
  "EVENT_BUS_NAME": "myapp-local-bus"
} }
```

SAM talks to the Docker API. With Podman, start its API socket and point SAM at it (`DOCKER_HOST`; on Windows/macOS use the URL shown by `podman machine inspect`). Podman also resolves `host.docker.internal`, but `--add-host host.docker.internal:host-gateway` may need `host.containers.internal` instead; use whichever your version resolves.

SAM applies `--env-vars` only to variables the function already declares in the template, and silently ignores the rest. That's why `synth:sam` declares `MONGO_URI` and `AWS_ENDPOINT_URL`: with plain `pnpm synth` they'd be dropped and the function would try real SSM through `MONGO_URI_PARAM`.

`directConnection=true` is needed because the replica-set member advertises `localhost`, which means something else inside the container. `sam local start-api` doesn't emulate HTTP API JWT authorizers, which is why the dev server exists.

---

## 14. Testing strategy

| Layer | Tool | What it proves |
|---|---|---|
| **Unit** | Vitest | Domain logic and Zod schemas, with no I/O. |
| **Handler integration** | Vitest + `MongoMemoryReplSet` + `aws-sdk-client-mock` | Handlers with real Mongo (including transactions) and mocked AWS calls. |
| **Local end to end** | `pnpm dev` + Floci | The full flow: API → SQS → worker → EventBridge → worker, plus DLQ behavior. |
| **Artifact** | `sam local invoke` | The bundle works in the real Lambda runtime. |
| **Cloud smoke** | A deployed `dev` stage | IAM, triggers, authorizer and Atlas networking, all within free tiers. |

**`services/vitest.config.ts`**

```ts
import { defineConfig } from 'vitest/config';
export default defineConfig({
  test: { globalSetup: ['test/global-setup.ts'], setupFiles: ['test/setup-env.ts'], testTimeout: 20_000 },
});
```

**`services/test/global-setup.ts`**

```ts
import type { TestProject } from 'vitest/node';
import { MongoMemoryReplSet } from 'mongodb-memory-server';

export default async function setup(project: TestProject) {
  const rs = await MongoMemoryReplSet.create({ replSet: { count: 1 } });
  project.provide('mongoUri', rs.getUri());
  return async () => { await rs.stop(); };
}

declare module 'vitest' {
  export interface ProvidedContext { mongoUri: string }
}
```

**`services/test/setup-env.ts`**

```ts
import { inject } from 'vitest';
process.env.MONGO_URI = inject('mongoUri');
process.env.MONGO_DB_NAME = `test_${process.env.VITEST_POOL_ID ?? '0'}`;
process.env.ORDERS_QUEUE_URL = 'https://sqs.local/000000000000/orders';
process.env.EVENT_BUS_NAME = 'test-bus';
```

**`services/test/orders.test.ts`**

```ts
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { mockClient } from 'aws-sdk-client-mock';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
import type { APIGatewayProxyEventV2WithJWTAuthorizer, SQSEvent } from 'aws-lambda';
import { handler as ordersApi } from '../src/functions/orders-api';
import { handler as ordersWorker } from '../src/functions/orders-worker';
import { closeMongo, getDb } from '../src/lib/mongo';
import { ensureIndexes } from '../src/lib/schema';
import { fakeContext } from '../../local/fake-context';

const sqsMock = mockClient(SQSClient);
const ebMock = mockClient(EventBridgeClient);

const apiEvent = (routeKey: string, body?: unknown, perms = ['read:orders', 'write:orders']) =>
  ({
    routeKey,
    body: body ? JSON.stringify(body) : undefined,
    isBase64Encoded: false,
    headers: {},
    requestContext: { requestId: 'req-1', authorizer: { jwt: { claims: { sub: 'user-1', permissions: `[${perms.join(' ')}]` }, scopes: [] } } },
  }) as unknown as APIGatewayProxyEventV2WithJWTAuthorizer;

const sqsEvent = (...bodies: unknown[]): SQSEvent => ({
  Records: bodies.map((b, i) => ({ messageId: `m-${i}`, body: JSON.stringify(b), messageAttributes: {} }) as never),
});

// Same collections and indexes as a migrated environment, so transactions never create collections.
beforeAll(async () => ensureIndexes(await getDb()));

beforeEach(() => {
  sqsMock.reset();
  ebMock.reset();
  sqsMock.on(SendMessageCommand).resolves({ MessageId: 'x' });
  ebMock.on(PutEventsCommand).resolves({ FailedEntryCount: 0, Entries: [{ EventId: 'e' }] });
});
afterAll(closeMongo);

describe('orders', () => {
  it('rejects users without write permission', async () => {
    const res = await ordersApi(apiEvent('POST /orders', { customerId: 'c', items: [{ sku: 'a', qty: 1 }] }, ['read:orders']), fakeContext('t'));
    expect(res.statusCode).toBe(403);
  });

  it('creates an order and publishes order.created', async () => {
    const res = await ordersApi(apiEvent('POST /orders', { customerId: 'c', items: [{ sku: 'a', qty: 1 }] }), fakeContext('t'));
    expect(res.statusCode).toBe(201);
    expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(1);
  });

  it('reports only the bad record as failed, and is idempotent', async () => {
    const created = await ordersApi(apiEvent('POST /orders', { customerId: 'c', items: [{ sku: 'a', qty: 1 }] }), fakeContext('t'));
    const { id } = JSON.parse(created.body!);
    const good = { type: 'order.created', version: 1, orderId: id, occurredAt: new Date().toISOString() };

    const out = await ordersWorker(sqsEvent(good, { bad: true }, good), fakeContext('w'));
    expect(out.batchItemFailures).toEqual([{ itemIdentifier: 'm-1' }]);
    expect(ebMock.commandCalls(PutEventsCommand)).toHaveLength(1); // the duplicate was skipped
  });
});
```

---

## 15. Deployment and CI/CD

### 15.1 One-time setup per environment

1. **AWS account:** turn on MFA for root, create an admin IAM Identity Center user, and **create a budget alert** (§18).
2. **Atlas:** create a Free cluster **in the same cloud region as your Lambdas** (for example, AWS `us-east-1`).
   - Create a database user with `readWrite` on `myapp` only, and a long random password.
   - Under Network Access, allow `0.0.0.0/0` (required for Lambda without private networking; see §17).
3. **Store the connection string** in SSM as a SecureString:
   ```bash
   aws ssm put-parameter --name /myapp/dev/mongo-uri --type SecureString \
     --value 'mongodb+srv://app:<password>@cluster0.xxxxx.mongodb.net/myapp?retryWrites=true&w=majority&appName=myapp'
   ```
4. **Run migrations** against Atlas: `MONGO_URI='...' MONGO_DB_NAME=myapp pnpm tsx scripts/migrate.ts`.
5. **Deploy:** `pnpm --filter infra exec cdk bootstrap`, then `pnpm --filter infra exec cdk deploy -c stage=dev`.
6. **Point the React app** (`VITE_API_URL`) at the `ApiUrl` output, and add the deployed web origin to Auth0's allowed URLs.

### 15.2 GitHub Actions (no long-lived AWS keys)

- Use **OIDC**: create an IAM role trusted by `token.actions.githubusercontent.com`, scoped to your repo and branch, and use `aws-actions/configure-aws-credentials` with `role-to-assume`.
- **Pipeline:** `pnpm install --frozen-lockfile` → `typecheck` → `test` → `cdk synth` → `cdk diff` → `cdk deploy` (dev automatically, prod on tag or manual approval).
- Run **migrations before deploying** code that depends on new indexes.

### 15.3 Safe changes to messages

- **Add fields** as optional; consumers ignore unknown fields.
- **For breaking changes,** bump `version`. Deploy consumers that accept both versions *first*, then producers.
- **Before removing a queue,** stop producers and drain both the queue and its DLQ.

---

## 16. Observability

This section has two parts:

- **16.1 Baseline:** CloudWatch, always on and free. It stays regardless of which vendor you pick.
- **16.2–16.11 Comparison:** New Relic vs Grafana Cloud, both on free tiers, fed by the **same** OpenTelemetry data. You run it in the `dev` stage for 2–4 weeks, then keep one.

### 16.1 Baseline (CloudWatch, always on)

- **Logs:** Powertools Logger produces JSON with function context. Always include `orderId`, `messageId` and `correlationId`.
- **Correlation:** pass the API `requestId` as the `correlationId` SQS message attribute, and log it in workers. For EventBridge, put it in `detail`.
- **Tracing:** X-Ray active tracing is enabled in the CDK. Powertools Tracer can also trace SDK clients if you want deeper traces.
- **Metrics to watch:**
  - Lambda: `Errors`, `Throttles`, `Duration` p95, `ConcurrentExecutions`
  - SQS: `ApproximateAgeOfOldestMessage`, DLQ depth
  - Atlas (free alerts): connections and ops per second
- **Business metrics:** Powertools Metrics (EMF) is free up to 10 custom metrics.
- **Log Insights query** for errors:
  ```
  fields @timestamp, level, message, orderId, correlationId | filter level = "ERROR" | sort @timestamp desc | limit 50
  ```

### 16.2 Comparison architecture: instrument once, send to both

Install **one** set of instrumentation (OpenTelemetry) and fan the same traces out to both vendors. Two vendor agents in every Lambda would double the overhead and make the comparison unfair.

```
Lambda function
 ├─ OpenTelemetry Node.js layer   auto-traces: Lambda invocation, AWS SDK (SQS, EventBridge, SSM), MongoDB, HTTP
 └─ OpenTelemetry Collector layer (Lambda extension)
       ├─ otlphttp/newrelic ─▶ https://otlp.nr-data.net            header  api-key: <ingest license key>
       └─ otlphttp/grafana  ─▶ https://otlp-gateway-<region>.grafana.net/otlp   header  Authorization: Basic base64(<instance id>:<token>)

CloudWatch (Lambda + SQS + DLQ metrics, Lambda logs)
 ├─ Grafana Cloud: CloudWatch data source, queried in place (nothing copied)
 └─ New Relic: AWS integration (API polling) for metrics + log-ingestion Lambda for logs

React SPA
 ├─ New Relic Browser agent   (npm @newrelic/browser-agent)
 └─ Grafana Faro              (npm @grafana/faro-react)
```

**Design choices**

| Choice | Why |
|---|---|
| **Traces only through OpenTelemetry** | It's the part that differs most between vendors. Metrics and logs already live in CloudWatch. |
| **No OTel metrics from Lambda** | New Relic prefers *delta* metric temporality while Grafana's gateway expects *cumulative*. Exporting one stream to both causes subtle problems, and CloudWatch already has the Lambda/SQS metrics. |
| **Comparison in `dev` only** | Keeps production lean. A config flag turns it on or off per stage. |
| **Ingest-only credentials** | Both keys can only *write* telemetry, which limits the damage if they leak (§16.4). |
| **X-Ray set to pass-through while OTel is on** | Avoids paying for two tracing systems on every invocation. |

### 16.3 Accounts and free-tier limits

| | New Relic (Free) | Grafana Cloud (Free) |
|---|---|---|
| Sign-up | newrelic.com, no credit card | grafana.com, no credit card |
| Data allowance | 100 GB/month, all data types combined | 10k metric series, 50 GB logs, 50 GB traces per month |
| Users | 1 full platform user + unlimited basic users | 3 active users |
| Retention | 8 days minimum (logs 30 days) | 14 days |
| At the limit | **Ingest stops** until next month (no surprise bill) | Must upgrade to Pro to send more |
| Usage view | *Administration → Plan & usage* (emails at 85%) | *Administration → Cost management / Usage* |

**Expected volume for this app** (rough): a trace for one invocation is a few KB, so 300k invocations a month at 100% sampling is about 1–3 GB of traces. Logs depend on how chatty you are, typically a few GB. That's well inside both free tiers.

### 16.4 Credentials

Create the credentials once per vendor.

**New Relic**
1. *Integrations & Agents → OpenTelemetry* (or *API keys*): create an **Ingest – License** key. This is what OTLP needs; a User API key won't work.
2. Note your **account region**: US uses `https://otlp.nr-data.net`, EU uses `https://otlp.eu01.nr-data.net`.

**Grafana Cloud**
1. Open your stack → *Connections → OpenTelemetry (OTLP)*. Copy the **OTLP endpoint** (ends in `/otlp`) and the numeric **Instance ID**.
2. Create an **access policy token** with only `traces:write`.
3. Build the Basic-auth value (instance ID, a colon, then the token, base64-encoded):
   ```bash
   echo -n "<instance-id>:<token>" | base64
   ```
   A common mistake is pasting the raw `glc_…` token; it must be the encoded `id:token` pair.

**Where to keep them**

The Lambda collector layer reads its config from files, environment variables, HTTP or S3, not from SSM. The pragmatic setup for a dev comparison:

- Store both values as **CI secrets** (GitHub Actions secrets): `NEW_RELIC_LICENSE_KEY` and `GRAFANA_OTLP_AUTH`.
- CDK reads them at deploy time and sets them as Lambda environment variables (encrypted at rest).
- **Trade-off:** they also appear in the synthesized CloudFormation template. Mitigate by using ingest-only keys (above), restricting who can read templates and function config, and **rotating both keys when the trial ends**.
- If that's not acceptable, store them in Secrets Manager and use a CloudFormation dynamic reference, which costs about $0.40 per secret per month.

### 16.5 Collector config with two exporters

**`infra/otel/collector.yaml`**

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: localhost:4317
      http:
        endpoint: localhost:4318

processors:
  # Lets the function return while the extension finishes exporting.
  decouple: {}

exporters:
  otlphttp/newrelic:
    endpoint: https://otlp.nr-data.net          # EU accounts: https://otlp.eu01.nr-data.net
    headers:
      api-key: ${env:NEW_RELIC_LICENSE_KEY}
  otlphttp/grafana:
    endpoint: ${env:GRAFANA_OTLP_ENDPOINT}       # https://otlp-gateway-prod-<region>.grafana.net/otlp
    headers:
      Authorization: Basic ${env:GRAFANA_OTLP_AUTH}

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [decouple]
      exporters: [otlphttp/newrelic, otlphttp/grafana]
```

Both exporters receive the same spans. If one vendor is down or rejects data, the other still gets it. Look for exporter errors in the function's CloudWatch logs (set `OPENTELEMETRY_EXTENSION_LOG_LEVEL=debug` temporarily while setting up).

### 16.6 CDK changes

**1. Config in `infra/cdk.json`** (dev stage only)

Find the latest layer versions on the OpenTelemetry Lambda releases page (link in §20) and fill in `<VERSION>`. The collector layer's architecture **must match** the functions (`arm64` here).

Leave `enabled` at `false` for the week-0 baseline (§16.10) and flip it to `true` for weeks 1–3. While it's `true`, every `cdk synth`/`deploy`, including CI's, needs `NEW_RELIC_LICENSE_KEY` and `GRAFANA_OTLP_AUTH` exported, or synth fails on purpose. `pnpm synth:sam` always skips OTel.

```json
"dev": {
  "region": "us-east-1",
  "...": "...",
  "otel": {
    "enabled": false,
    "samplingRatio": 1,
    "nodejsLayerArn": "arn:aws:lambda:us-east-1:184161586896:layer:opentelemetry-nodejs-<VERSION>:1",
    "collectorLayerArn": "arn:aws:lambda:us-east-1:184161586896:layer:opentelemetry-collector-arm64-<VERSION>:1",
    "grafanaOtlpEndpoint": "https://otlp-gateway-prod-us-east-0.grafana.net/otlp",
    "newRelicIntegration": { "awsAccountId": "<shown in the New Relic AWS wizard>", "externalId": "<shown in the wizard>" },
    "grafanaIntegration": { "awsAccountId": "<shown on the Grafana CloudWatch data source page>", "externalId": "<shown there>" },
    "newRelicLogForwarderArn": "<ARN of the deployed newrelic-log-ingestion function, or omit>"
  }
}
```

**2. Helper: `infra/lib/otel.ts`**

```ts
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
```

**3. Wire it into `AppStack`**

Add `otel?: OtelConfig` to `AppStackProps`, pass `otel: app.node.tryGetContext('samLocal') === 'true' ? undefined : cfg.otel` from `bin/app.ts`, then change the function factory:

```ts
import { otelBundling, otelEnvironment, otelLayers, type OtelConfig } from './otel';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as destinations from 'aws-cdk-lib/aws-logs-destinations';

// inside the constructor, before `fn`:
const otel = props.otel?.enabled ? props.otel : undefined;
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
    retention: logs.RetentionDays.TWO_WEEKS,
    removalPolicy: RemovalPolicy.DESTROY,
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
      externalModules: ['@aws-sdk/*', 'kerberos', 'snappy', '@mongodb-js/zstd', 'mongodb-client-encryption', 'gcp-metadata', 'socks', 'aws4'],
      ...(otel ? otelBundling() : {}),
    },
    ...extra,
    environment: {
      ...commonEnv,
      POWERTOOLS_SERVICE_NAME: name,
      ...(otel ? otelEnvironment(otel, name, props.stage) : {}),
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
```

Also let the browser send trace headers through CORS by extending `allowHeaders` on the HTTP API:

```ts
allowHeaders: ['authorization', 'content-type', 'traceparent', 'tracestate', 'newrelic'],
```

**4. Read-only IAM roles for the two vendors** (same stack, only when `otel` is set)

Both vendors read CloudWatch by assuming a role in your account. Their account ID and your external ID are shown in each vendor's setup screen.

```ts
const vendorRole = (id: string, i: { awsAccountId: string; externalId: string }, policy: string) =>
  new iam.Role(this, id, {
    roleName: `${prefix}-${id}`,
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
```

**5. Deploy**

```bash
export NEW_RELIC_LICENSE_KEY=...    # from CI secrets in the pipeline
export GRAFANA_OTLP_AUTH=...
pnpm --filter infra exec cdk deploy -c stage=dev
```

To turn the comparison off, set `"enabled": false` and deploy again. The layers, environment variables, subscription filters and roles are all removed.

### 16.7 Vendor-side setup

**New Relic**
1. **AWS metrics:** *Integrations & Agents → Amazon Web Services → Link account*. Choose **API polling** (not Metric Streams, which bills you for Firehose and metric updates). Paste `NewRelicRoleArn`. Enable at least the Lambda and SQS integrations.
2. **Logs:** from the AWS Serverless Application Repository, deploy **NewRelic-log-ingestion** with your license key. Deploy it in the same account and region as the stack. Put its ARN in `newRelicLogForwarderArn` and redeploy; CDK adds the subscription filters and the invoke permissions they need. Each log group allows only 2 subscription filters, so keep the second slot free.
3. **Traces:** appear automatically under *APM & Services* (as OpenTelemetry services) once traffic flows.
4. **Alerts:** create alert conditions for errors, latency and DLQ depth (from the AWS integration's SQS data) with an email destination.

**Grafana Cloud**
1. **CloudWatch data source:** *Connections → Data sources → CloudWatch*. Choose **Grafana Assume Role** authentication, paste `GrafanaRoleArn`, set the default region. Test it with a Lambda `Invocations` query.
2. **Dashboards:** import the community *AWS Lambda* and *AWS SQS* dashboards for the CloudWatch data source, or build one panel each for invocations, errors, duration p95, queue age and DLQ depth.
3. **Traces:** in *Explore → Tempo*, search by `service.name`. *Application Observability* adds service maps and RED metrics; those generated metrics count toward the 10k-series allowance.
4. **Logs:** in *Explore → CloudWatch Logs*, run Logs Insights queries against your log groups. Add a derived field on `trace.id` so log lines link to their trace.
5. **Alerts:** create Grafana alert rules on CloudWatch queries (DLQ ≥ 1, error count, queue age) with an email contact point.

### 16.8 Link logs to traces

Add trace IDs to log lines so both tools can jump from a log entry to its trace. `@opentelemetry/api` is tiny and does nothing when the layer isn't present.

```bash
pnpm --filter services add @opentelemetry/api
```

**`services/src/lib/observability.ts`** (addition)

```ts
import { trace } from '@opentelemetry/api';

/** `trace.id` / `span.id` are the attribute names New Relic uses for log-trace correlation. */
export function traceFields(): Record<string, string> {
  const ctx = trace.getActiveSpan()?.spanContext();
  return ctx ? { 'trace.id': ctx.traceId, 'span.id': ctx.spanId } : {};
}
```

**Usage**

```ts
logger.info('Processed order.created', { ...traceFields(), orderId: msg.orderId, outcome, correlationId });
```

In an SQS worker, `traceFields()` returns the **worker invocation's** trace. One invocation handles up to 10 records from different API requests, so every record in a batch logs the same `trace.id`, and it isn't the producer's. Each record's own producer context is in its `traceparent` message attribute. Join producer and consumer logs on `correlationId`, which is per record and survives every hop.

**What propagates automatically, and what doesn't**
- **API → SQS → worker:** the AWS SDK instrumentation adds trace context to SQS message attributes. The worker invocation is a separate trace, at best *linked* to the producer spans, never a child of one request. How each vendor shows those links is worth scoring.
- **EventBridge:** context does **not** propagate through the bus. orders-worker copies `correlationId` into the event `detail` and notifications-worker logs it. Use it to connect these traces in both tools.
- **SQS attribute limit:** SQS allows 10 message attributes. The instrumentation uses one or two, and you use one (`correlationId`), so this is fine.

### 16.9 React: both browser agents

Run both in `dev` only. In production keep the winner, because two agents mean two scripts and two sets of beacons.

```bash
pnpm --filter web add @newrelic/browser-agent @grafana/faro-react @grafana/faro-web-tracing
```

**`apps/web/.env.development`** (additions)

```
VITE_RUM=both                      # none | newrelic | grafana | both
VITE_NR_ACCOUNT_ID=...
VITE_NR_TRUST_KEY=...
VITE_NR_AGENT_ID=...
VITE_NR_APPLICATION_ID=...
VITE_NR_BROWSER_KEY=NRJS-...       # browser key, public by design (not the ingest license key)
VITE_FARO_URL=https://faro-collector-prod-<region>.grafana.net/collect/<app-key>
VITE_API_ORIGIN_REGEX=^https://[a-z0-9]+\.execute-api\.us-east-1\.amazonaws\.com
```

Get the New Relic values from *Add data → Browser monitoring → Copy/paste* for a new app. Get the Faro URL from *Frontend Observability → Create app* in Grafana Cloud.

**`apps/web/src/observability.ts`**

```ts
import { BrowserAgent } from '@newrelic/browser-agent/loaders/browser-agent';
import { getWebInstrumentations, initializeFaro, ReactIntegration } from '@grafana/faro-react';
import { TracingInstrumentation } from '@grafana/faro-web-tracing';

const mode = import.meta.env.VITE_RUM ?? 'none';
const apiOrigin = new RegExp(import.meta.env.VITE_API_ORIGIN_REGEX ?? '^$');
// New Relic matches exact origins. In dev VITE_API_URL is "/api" (same origin, always allowed).
const apiOriginUrl = new URL(import.meta.env.VITE_API_URL, window.location.href).origin;

if (mode === 'newrelic' || mode === 'both') {
  const key = import.meta.env.VITE_NR_BROWSER_KEY;
  const appId = import.meta.env.VITE_NR_APPLICATION_ID;
  new BrowserAgent({
    init: {
      // Only one agent should inject trace headers, or they overwrite each other.
      // Cross-origin calls (CloudFront → execute-api) only get trace headers if listed here.
      distributed_tracing: {
        enabled: mode === 'newrelic',
        allowed_origins: [apiOriginUrl],
        cors_use_newrelic_header: true,
        cors_use_tracecontext_headers: true,
      },
      privacy: { cookies_enabled: true },
      ajax: { deny_list: ['bam.nr-data.net'] },
    },
    info: { beacon: 'bam.nr-data.net', errorBeacon: 'bam.nr-data.net', licenseKey: key, applicationID: appId, sa: 1 },
    loader_config: {
      accountID: import.meta.env.VITE_NR_ACCOUNT_ID,
      trustKey: import.meta.env.VITE_NR_TRUST_KEY,
      agentID: import.meta.env.VITE_NR_AGENT_ID,
      licenseKey: key,
      applicationID: appId,
    },
  });
}

if (mode === 'grafana' || mode === 'both') {
  initializeFaro({
    url: import.meta.env.VITE_FARO_URL,
    app: { name: 'myapp-web', version: import.meta.env.VITE_APP_VERSION ?? 'dev', environment: import.meta.env.MODE },
    instrumentations: [
      ...getWebInstrumentations(),
      new TracingInstrumentation({ instrumentationOptions: { propagateTraceHeaderCorsUrls: [apiOrigin] } }),
      new ReactIntegration(),
    ],
  });
}
```

Import it **first** in `main.tsx` so it captures early errors:

```tsx
import './observability';
import React from 'react';
// ...rest unchanged
```

- **Trace header ownership:** with `both`, Faro owns the `traceparent` header and New Relic's distributed tracing is off. Switch `VITE_RUM` to `newrelic` for a few days to test New Relic's browser-to-backend linking.
- **Privacy:** don't put emails or names in custom attributes. Use Faro's `beforeSend` and New Relic's obfuscation rules if URLs can contain personal data.

### 16.10 Running the comparison

**Week 0: baseline.** Deploy `dev` with `otel.enabled: false` and run the load test below. Record cold start and duration numbers.

**Weeks 1–3: both vendors on.** Run the same load test, then the exercises.

**Load test with k6** (free, open source). Get a token by signing in to the dev SPA and copying an access token, for example from the network tab.

```js
// load/orders.js   →   k6 run -e API_URL=https://xxx.execute-api.us-east-1.amazonaws.com -e TOKEN=... load/orders.js
import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = { vus: 5, duration: '10m' }; // ≈ 6k requests: safe for Atlas M0 and all free tiers

const headers = { Authorization: `Bearer ${__ENV.TOKEN}`, 'Content-Type': 'application/json' };

export default function () {
  const res = http.post(`${__ENV.API_URL}/orders`, JSON.stringify({ customerId: 'k6', items: [{ sku: 'A-1', qty: 1 }] }), { headers });
  check(res, { created: (r) => r.status === 201 });
  http.get(`${__ENV.API_URL}/orders`, { headers });
  sleep(1);
}
```

**Cold start and overhead** (CloudWatch Logs Insights, run on each function's log group, before vs after):

```
filter @type = "REPORT"
| stats count(*) as invocations,
        count(@initDuration) as coldStarts,
        avg(@initDuration) as avgInitMs, pct(@initDuration, 95) as p95InitMs,
        avg(@duration) as avgMs, pct(@duration, 95) as p95Ms,
        max(@maxMemoryUsed / 1000 / 1000) as maxMemMB
  by bin(1d)
```

**Exercises: time how long each tool takes to give you the answer**

| # | Exercise | How to trigger it (dev only) | The question to answer |
|---|---|---|---|
| 1 | Poison message | Send `{"bad":true}` to the orders queue | Which message failed, why, and that it reached the DLQ |
| 2 | Slow request | Temporarily add a 1.5 s delay in `listOrders` and deploy | Which span is slow, and on which route |
| 3 | Database outage | Point the dev SSM Mongo URI at a wrong password for 10 minutes | How fast do you get alerted, and is the cause obvious |
| 4 | Cold starts | Deploy any change, then run the load test | Are cold starts visible and separated from warm calls |
| 5 | End-to-end flow | Create one order in the UI | Can you follow browser → API → SQS → worker → EventBridge → worker |
| 6 | Frontend error | Throw an error from a button handler | Stack trace, browser, user session, and the API call before it |

**Scorecard** (score each 1–5; weight to taste)

| Criterion | Weight | New Relic | Grafana Cloud |
|---|---|---|---|
| Setup effort (hours to first useful trace, alert, dashboard) | 2 | | |
| Trace view across API → SQS → worker | 3 | | |
| Time to answer exercises 1–6 | 3 | | |
| Alert quality (setup, noise, clarity of the notification) | 2 | | |
| Dashboards and day-to-day usability | 2 | | |
| Frontend monitoring | 1 | | |
| Team access on the free tier (1 full user vs 3 users) | 2 | | |
| Overhead (cold start and duration delta, measured above) | 2 | | |
| Projected cost at 10× your current volume | 2 | | |
| Lock-in (how easy to leave) | 1 | | |

The Lambda overhead is shared because the same layers feed both vendors, so it doesn't separate them. It tells you what keeping *either* will cost, versus CloudWatch and X-Ray alone.

### 16.11 After the comparison

**Keep the winner:**
- **Grafana:** remove the `otlphttp/newrelic` exporter, the New Relic role, the log forwarder and subscription filters, and the New Relic browser agent.
- **New Relic:** remove `otlphttp/grafana`, the Grafana role and Faro.

**Then:**
- **Rotate** the remaining key and **revoke** the other vendor's key.
- **Decide whether production gets OpenTelemetry at all.** If you enable it in `prod`, start with `samplingRatio` around 0.1–0.25 and watch the free-tier usage pages for a month.
- **Keep the CloudWatch alarms from §12** either way. They sit closest to the source and don't depend on a third party.

---

## 17. Security

**Identity and API**
- Use RS256 tokens with short lifetimes (1 h), refresh-token rotation, and disabled public sign-ups for internal apps.
- The JWT authorizer validates the issuer, audience and expiry. **Handlers enforce permissions and ownership** on every route.
- Set CORS to your exact web origin, never `*` with credentials.
- Use stage throttling (included). AWS WAF costs money; add it if you outgrow throttling.

**AWS**
- Use **least-privilege IAM** via CDK `grant*` methods, with no wildcard policies.
- Keep secrets in **SSM SecureString**, never in code, Git or plain environment variables.
- Keep SQS encrypted (SSE-SQS) with `enforceSSL`.
- Enable CloudTrail (the free management-event trail) and MFA everywhere.

**MongoDB on the free tier: the `0.0.0.0/0` trade-off**

Atlas Free supports neither private endpoints nor VPC peering, and Lambda outside a VPC has no fixed IP. So the IP allowlist must be open. Compensate with:
- TLS (on by default with `mongodb+srv`).
- A dedicated database user per environment with `readWrite` on the app database only, never Atlas admin.
- A 32+ character random password stored in SSM, rotated periodically.
- Atlas alerts for connection spikes.

For production on a dedicated cluster, move to **PrivateLink + Lambda in a VPC**. That adds VPC interface endpoints for SQS, SSM and EventBridge, which cost money.

**Data and code**
- Validate all inputs (Zod), cap list sizes and string lengths, and never pass raw user input as a Mongo query object. This prevents operator injection such as `{"$gt": ""}`.
- Never log tokens, passwords or full PII documents.
- Use Dependabot or Renovate, plus `pnpm audit` in CI.

---

## 18. Cost guardrails

- **AWS Budgets:** set an alert at $1 and $10 per month as an early warning.
- **Log retention:** set on every log group (done in the CDK).
- **Avoid these, which have no free tier:**
  - NAT Gateway
  - VPC interface endpoints
  - Secrets Manager
  - Customer-managed KMS keys
  - Provisioned concurrency
  - SQS provisioned mode
  - WAF
  - Atlas paid tiers until you need them
- **Few triggered queues:** each SQS trigger polls continuously, so add queues per domain, not per message type. DLQs have no trigger and don't poll.
- **Batch sends:** use `SendMessageBatch` and `PutEvents` with multiple entries when one call produces many messages. The outbox publishes per row on purpose (§8.2).
- **Sweeper schedule:** every 5 minutes is about 8,640 invocations per month, which is negligible. Don't run it every minute in the cloud.
- **X-Ray:** lower the sampling rate if you approach 100k traces per month.
- **Vendor comparison (§16):** New Relic stops ingesting at its free limit rather than billing, and Grafana needs an explicit upgrade, so neither can surprise you. On the AWS side, expect a few cents for CloudWatch API polling (`GetMetricData` isn't in the free allowance) and for Logs Insights queries run from Grafana (billed per GB scanned). Avoid CloudWatch Metric Streams, which bill per metric update plus Firehose.

---

## 19. Checklists

### Lambda
- [ ] `nodejs24.x`, arm64, bundled and minified, source maps on
- [ ] SDK clients and Mongo client at module scope; `warmMongo()` in every function file
- [ ] Timeouts: API ≤ 10 s, workers 30 s; memory 512 MB to start, then tuned
- [ ] JSON logs, log level set, log retention set
- [ ] Inputs validated with Zod; errors mapped to 4xx, unknown errors to 500 and logged
- [ ] Async-invoked functions have an explicit `retryAttempts` and failure handling (destination or alarm)
- [ ] Account concurrency quota checked (new accounts can be low)

### SQS
- [ ] Every queue has a DLQ, `maxReceiveCount` 3–5, DLQ retention 14 days
- [ ] Visibility timeout = 6× function timeout
- [ ] `reportBatchItemFailures: true` + Powertools `BatchProcessor`
- [ ] `maxConcurrency` sized to the Mongo connection and ops budget
- [ ] Consumers idempotent on a **business key**
- [ ] Alarms on DLQ depth and oldest-message age
- [ ] No filter criteria that silently delete needed messages

### EventBridge
- [ ] Custom bus, versioned `detail`, `source` = `myapp.<domain>`
- [ ] Rules target SQS (not Lambda) with a target DLQ and retry policy
- [ ] `FailedEntryCount` checked on every `PutEvents`
- [ ] Archive enabled with retention

### MongoDB
- [ ] Cached client, small `maxPoolSize`, timeouts below the Lambda timeout
- [ ] Connection math done and kept under ~70% of the cluster limit
- [ ] Indexes created by migration; every query uses an index
- [ ] Transactions only where several documents must change together; outbox for messages
- [ ] TTL indexes on `processed_messages` and sent `outbox` rows
- [ ] Least-privilege database user; URI stored in SSM SecureString

### Auth0
- [ ] API with RBAC + permissions in the token; 1 h token lifetime
- [ ] SPA app with exact callback, logout and origin URLs; refresh token rotation
- [ ] Sign-ups disabled for an internal app; roles assigned
- [ ] Custom domain configured (or a conscious token-storage decision)
- [ ] Separate dev and prod setup

### Local and testing
- [ ] `pnpm local:up && pnpm local:bootstrap && pnpm migrate && pnpm dev` works on a clean machine
- [ ] Offline auth mode (`AUTH_MODE=local`) works for curl and tests
- [ ] Tests cover permission denial, partial batch failure and duplicate delivery
- [ ] `sam local invoke` passes on the bundled worker before each release

### Observability comparison
- [ ] Ingest-only keys created (New Relic Ingest – License; Grafana token with `traces:write` only) and stored as CI secrets
- [ ] Collector layer architecture matches the functions (arm64)
- [ ] `mongodb` shipped as a node_module when OTel is on, so its calls are traced
- [ ] Baseline cold start and duration recorded with OTel **off**
- [ ] Traces visible in both tools for API → SQS → worker
- [ ] New Relic: AWS integration on API polling, log forwarder subscribed
- [ ] Grafana: CloudWatch data source via assume role, Lambda/SQS dashboard, `trace.id` derived field
- [ ] Alerts for DLQ, errors and queue age set up in both, sending email
- [ ] Exercises 1–6 run and scorecard filled in
- [ ] Losing vendor removed, its key revoked, remaining key rotated

---

## 20. Sources

- AWS Lambda runtimes (Node.js 24 support and deprecation dates): https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html
- SQS triggers, scaling, max concurrency and provisioned mode: https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-scaling.html
- SQS trigger configuration (batch size, partial batch responses): https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html
- SQS 1 MiB message size: https://aws.amazon.com/about-aws/whats-new/2025/08/amazon-sqs-max-payload-size-1mib
- AWS Free Tier (always-free Lambda and SQS): https://aws.amazon.com/free/serverless
- API Gateway free tier on the new Free plan: https://builder.aws.com/content/3KlRhfU9w7K79H2be1uFAjiX62N/run-a-real-api-for-dollar0-a-month-on-lambdas-always-free-tier
- Atlas Free cluster limits: https://mongodb.com/docs/atlas/reference/free-shared-limitations/
- Atlas connection limits: https://www.mongodb.com/docs/atlas/reference/alert-resolutions/connection-alerts/
- Auth0 pricing: https://www.auth0.com/pricing
- LocalStack account requirement (March 2026): https://dev.to/toddyholiday/localstack-went-paid-what-the-alternatives-get-right-and-what-still-comes-down-to-production-5bnf
- Floci (MIT local AWS emulator): https://github.com/floci-io/floci
- OpenTelemetry Lambda layers (layer ARNs and releases): https://github.com/open-telemetry/opentelemetry-lambda
- OpenTelemetry Lambda collector configuration (`OPENTELEMETRY_COLLECTOR_CONFIG_URI`): https://opentelemetry.io/docs/faas/lambda-collector/
- New Relic OTLP endpoints and `api-key` header: https://docs.newrelic.com/docs/opentelemetry/best-practices/opentelemetry-otlp/
- New Relic pricing and free tier: https://newrelic.com/info/pricing
- Grafana Cloud OTLP Basic auth (instance ID + token): https://community.grafana.com/t/opentelemetry-protocol-otlp-endpoint-credentials-for-grafana-cloud/105782
- Grafana Cloud free tier and Pro pricing: https://www.costbench.com/software/developer-tools/grafana/
