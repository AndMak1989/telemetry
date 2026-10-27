# Implementation Guide: Orders & Payments Practice System

Step-by-step guide for building one practice system that exercises **every topic** from Weeks 02–03.
Theory for each step lives in [`01-theory-explained.md`](01-theory-explained.md).

**Stack:** Node.js 20+ · NestJS · PostgreSQL 16 · Redis 7 · RabbitMQ 3.13 · TypeORM · Docker Compose · k6

Each step has: **Goal**, **Topics**, **Files**, **Key code**, **Verify**, **Interview talking points**.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Step 0. Prerequisites](#step-0-prerequisites)
- [Step 1. Scaffold the Nest Monorepo](#step-1-scaffold-the-nest-monorepo)
- [Step 2. Infrastructure with Docker Compose](#step-2-infrastructure-with-docker-compose)
- [Step 3. Data Modeling and Migrations](#step-3-data-modeling-and-migrations)
- [Step 4. Indexes and Slow Queries](#step-4-indexes-and-slow-queries)
- [Step 5. Transactions and Isolation Levels](#step-5-transactions-and-isolation-levels)
- [Step 6. Replication and Read/Write Split](#step-6-replication-and-readwrite-split)
- [Step 7. Partitioning and Sharding](#step-7-partitioning-and-sharding)
- [Step 8. Consensus and CAP (Experiment)](#step-8-consensus-and-cap-experiment)
- [Step 9. Caching with Redis](#step-9-caching-with-redis)
- [Step 10. RabbitMQ Messaging](#step-10-rabbitmq-messaging)
- [Step 11. Idempotency, Outbox, Inbox](#step-11-idempotency-outbox-inbox)
- [Step 12. Saga for Order Creation](#step-12-saga-for-order-creation)
- [Step 13. Timeouts, Retries, Backoff](#step-13-timeouts-retries-backoff)
- [Step 14. Circuit Breaker and Bulkhead](#step-14-circuit-breaker-and-bulkhead)
- [Step 15. Rate Limiting](#step-15-rate-limiting)
- [Step 16. Scaling and Graceful Degradation](#step-16-scaling-and-graceful-degradation)
- [Step 17. Chaos and Load Testing](#step-17-chaos-and-load-testing)
- [Step 18. Observability Minimum](#step-18-observability-minimum)
- [Appendix A. Topic → Step Map](#appendix-a-topic--step-map)
- [Appendix B. Final Demo Checklist](#appendix-b-final-demo-checklist)

---

## Architecture Overview

```mermaid
flowchart LR
  Client --> NGINX[nginx LB]
  NGINX --> GW[api-gateway<br/>rate limit, timeouts]
  GW --> OS[order-service<br/>saga orchestrator, cache, outbox]
  GW --> PS[payment-service<br/>idempotency, breaker]
  OS -- commands/events --> MQ[(RabbitMQ)]
  MQ --> PS
  MQ --> IS[inventory-service<br/>stock, isolation demos]
  MQ --> NS[notification-service<br/>inbox, DLQ]
  PS --> FP[fake-payment-provider<br/>flaky, slow]
  OS --> PGP[(Postgres primary)]
  PGP -. streaming .-> PGR[(Postgres replica)]
  OS --> PGR
  PS --> PGP
  IS --> PGP
  OS --> R[(Redis)]
  GW --> R
```

| Service | Port | Owns | Main topics |
|---|---|---|---|
| `api-gateway` | 3000 | nothing | rate limiting, deadlines, load shedding |
| `order-service` | 3001 | `orders` DB | data model, indexes, partitioning, replication, cache, outbox, saga |
| `payment-service` | 3002 | `payments` DB | idempotency, retries, circuit breaker, bulkhead |
| `inventory-service` | 3003 | `inventory` DB | transactions, isolation, locking |
| `notification-service` | 3004 | `notifications` DB | consumer idempotency (inbox), DLQ |
| `fake-payment-provider` | 3010 | none | failure injection |

Database-per-service: one Postgres instance, **separate databases** (`orders`, `payments`, `inventory`, `notifications`). Services never read each other's DB.

Final folder layout:
```
.
├── apps/
│   ├── api-gateway/
│   ├── order-service/
│   ├── payment-service/
│   ├── inventory-service/
│   ├── notification-service/
│   └── fake-payment-provider/
├── libs/
│   └── common/            # config, logger, http client, outbox, inbox, rabbit, redis helpers
├── infra/
│   ├── docker-compose.yml
│   ├── postgres/          # init scripts, replica setup
│   ├── nginx/nginx.conf
│   └── etcd/              # step 8
├── scripts/               # seed, race demos, chaos
├── load/                  # k6 scripts
└── nest-cli.json
```

---

## Step 0. Prerequisites

- Node.js 20 LTS+, npm (or pnpm)
- Docker Desktop (WSL2 backend on Windows)
- Nest CLI: `npm i -g @nestjs/cli`
- k6 (`winget install k6` / `brew install k6`)
- Optional: `psql`, RedisInsight, a REST client (Bruno/Postman/`curl`)

---

## Step 1. Scaffold the Nest Monorepo

**Goal:** one repo, several Nest apps, shared library.
**Topics:** foundation.

```bash
nest new orders-platform --package-manager npm
cd orders-platform
nest generate app order-service
nest generate app payment-service
nest generate app inventory-service
nest generate app notification-service
nest generate app fake-payment-provider
# rename default app to api-gateway in nest-cli.json or generate it and delete the default
nest generate library common

npm i @nestjs/config @nestjs/typeorm typeorm pg ioredis amqplib amqp-connection-manager \
      opossum axios uuid nestjs-pino pino-http class-validator class-transformer \
      @nestjs/schedule @nestjs/terminus prom-client etcd3
npm i -D @types/amqplib @types/opossum
```

> Note: for bulkheads this guide writes a tiny semaphore (Step 14) instead of `p-limit`, because `p-limit` v4+ is ESM-only and Nest builds CommonJS by default.
>
> Note: in monorepo mode Nest bundles each app with webpack, so non-TS files (e.g. `.lua`) are not copied and `__dirname` does not point at the source folder. Keep Lua scripts as TS string constants (Step 15).

`libs/common` contents (fill progressively):
- `config/` – typed env config (`DB_URL`, `DB_REPLICA_URL`, `REDIS_URL`, `AMQP_URL`)
- `logger/` – `nestjs-pino` with `correlationId`
- `middleware/correlation-id.middleware.ts` – reads/creates `X-Correlation-Id`
- `http/resilient-http.ts` – Step 13
- `messaging/` – Rabbit connection, publisher, consumer base – Step 10
- `outbox/`, `inbox/` – Step 11
- `util/` – small helpers used across steps:
  ```ts
  export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

  export function withTimeout<T>(p: Promise<T>, ms: number, what = 'operation'): Promise<T> {
    let t: NodeJS.Timeout;
    const timeout = new Promise<never>((_, reject) => {
      t = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms);
    });
    return Promise.race([p, timeout]).finally(() => clearTimeout(t));
  }
  ```

Correlation ID middleware:
```ts
import { Injectable, NestMiddleware } from '@nestjs/common';
import { randomUUID } from 'crypto';

@Injectable()
export class CorrelationIdMiddleware implements NestMiddleware {
  use(req: any, res: any, next: () => void) {
    const id = req.headers['x-correlation-id'] ?? randomUUID();
    req.correlationId = id;
    res.setHeader('x-correlation-id', id);
    next();
  }
}
```

**Verify:** `npm run start:dev order-service` returns `Hello World` on its port.

---

## Step 2. Infrastructure with Docker Compose

**Goal:** Postgres primary + streaming replica, Redis, RabbitMQ.
**Topics:** replication foundation (Step 6).

> Uses the official `postgres` image. (Bitnami's free image catalog was discontinued in 2025, so `bitnami/postgresql` tags may not pull anymore.) The replica is bootstrapped with `pg_basebackup`.

`infra/docker-compose.yml`:
```yaml
services:
  pg-primary:
    image: postgres:16
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app
      POSTGRES_DB: orders
    # async replication first; switched to sync in Step 6 via ALTER SYSTEM
    command: >
      postgres -c wal_level=replica -c max_wal_senders=10 -c max_replication_slots=10
      -c shared_preload_libraries=pg_stat_statements
    ports: ["5432:5432"]
    volumes:
      - ./postgres/init:/docker-entrypoint-initdb.d
      - pg-primary:/var/lib/postgresql/data

  pg-replica:
    image: postgres:16
    depends_on: [pg-primary]
    user: postgres
    environment:
      PGPASSWORD: repl
    # first start: clone primary (-R writes standby.signal + primary_conninfo, -C -S creates a replication slot)
    command: >
      bash -c "if [ ! -s /var/lib/postgresql/data/PG_VERSION ]; then
                 until pg_basebackup -h pg-primary -U repl -D /var/lib/postgresql/data -R -X stream -C -S replica1;
                 do echo 'waiting for primary'; sleep 2; done;
                 chmod 0700 /var/lib/postgresql/data;
               fi;
               exec postgres -c hot_standby=on"
    ports: ["5433:5432"]
    volumes:
      - pg-replica:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    command: ["redis-server", "--maxmemory", "256mb", "--maxmemory-policy", "allkeys-lru"]
    ports: ["6379:6379"]

  rabbitmq:
    image: rabbitmq:3.13-management
    environment:
      RABBITMQ_DEFAULT_USER: app
      RABBITMQ_DEFAULT_PASS: app
    ports: ["5672:5672", "15672:15672"]

volumes:
  pg-primary:
  pg-replica:
```

`infra/postgres/init/01-init.sh` (runs once, on first start of the primary):
```bash
#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
  CREATE ROLE repl WITH REPLICATION LOGIN PASSWORD 'repl';
  CREATE DATABASE payments;
  CREATE DATABASE inventory;
  CREATE DATABASE notifications;
  CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
EOSQL
# "all" in pg_hba does not match replication connections; allow them explicitly
echo "host replication repl all scram-sha-256" >> "$PGDATA/pg_hba.conf"
```
> On Windows, save the script with LF line endings, otherwise bash fails on `\r`.

**Verify:**
- `docker compose -f infra/docker-compose.yml up -d`
- `psql -h localhost -p 5432 -U app -d orders -c "select * from pg_stat_replication;"` shows one replica.
- RabbitMQ UI at http://localhost:15672 (app/app).

---

## Step 3. Data Modeling and Migrations

**Goal:** schemas per service, justified DB choice.
**Topics:** #1 Data models.

Use TypeORM migrations (`synchronize: false`). Example `order-service` schema:

```sql
CREATE TABLE orders (
  id           uuid        NOT NULL,
  customer_id  uuid        NOT NULL,
  status       text        NOT NULL CHECK (status IN ('PENDING','STOCK_RESERVED','PAYMENT_DEFERRED',
                                                      'CONFIRMED','COMPENSATING','CANCELLED')),  -- mirrors saga states (Step 12)
  total_cents  bigint      NOT NULL CHECK (total_cents >= 0),
  version      int         NOT NULL DEFAULT 0,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id, created_at)          -- partition key must be in PK (Step 7)
) PARTITION BY RANGE (created_at);

CREATE TABLE order_items (
  order_id    uuid   NOT NULL,
  sku         text   NOT NULL,
  qty         int    NOT NULL CHECK (qty > 0),
  price_cents bigint NOT NULL,
  PRIMARY KEY (order_id, sku)
);
```
> Lookups by `id` alone (without `created_at`) must check every partition. That is acceptable for this demo; in production use a time-ordered ID (UUIDv7, `uuidv7()` is built into Postgres 18) so the app can extract the timestamp from the ID and add a `created_at` range predicate (enabling partition pruning), or include `created_at` in the API.
>
> Start without partitioning if you prefer, and convert in Step 7. FK from `order_items` to a partitioned `orders` requires referencing the full PK, so we keep the relation logical (validated in app) to keep it simple.

`inventory-service`:
```sql
CREATE TABLE stock (
  sku      text PRIMARY KEY,
  qty      int  NOT NULL CHECK (qty >= 0),
  version  int  NOT NULL DEFAULT 0
);
CREATE TABLE reservations (
  order_id uuid NOT NULL,
  sku      text NOT NULL,
  qty      int  NOT NULL,
  status   text NOT NULL,            -- RESERVED | RELEASED
  PRIMARY KEY (order_id, sku)
);
```

`payment-service`:
```sql
CREATE TABLE payments (
  id          uuid PRIMARY KEY,
  order_id    uuid NOT NULL UNIQUE,            -- one payment per order: natural idempotency
  amount_cents bigint NOT NULL,
  status      text NOT NULL,                   -- PENDING | SUCCEEDED | FAILED | REFUNDED
  provider_ref text,
  created_at  timestamptz NOT NULL DEFAULT now()
);
```

Write a short `docs/adr-001-datastores.md` (optional) answering: why relational for orders/payments (transactions, constraints, joins for reporting), why Redis for cache/limits (key-value, TTL, atomic ops), why not MongoDB (no need for schemaless aggregates; `jsonb` covers flexible fields).

**Verify:** `npm run migration:run` per service; `\d+ orders` in psql.

**Talking points:** access patterns drive choice; money needs constraints + transactions; polyglot only where it pays.

---

## Step 4. Indexes and Slow Queries

**Goal:** see a slow query, explain it with `EXPLAIN ANALYZE`, fix with the right index.
**Topics:** #2 Indexes.

1. Create monthly partitions **before** seeding (`scripts/create-partitions.sql`). Do not rely on a `DEFAULT` partition for real data: once it holds rows for a month, `CREATE TABLE ... PARTITION OF` for that month fails (see Step 7).
   ```sql
   DO $$
   DECLARE m date := date_trunc('month', now() - interval '12 months');
   BEGIN
     WHILE m <= date_trunc('month', now() + interval '1 month') LOOP
       EXECUTE format('CREATE TABLE IF NOT EXISTS orders_%s PARTITION OF orders FOR VALUES FROM (%L) TO (%L)',
                      to_char(m, 'YYYY_MM'), m, m + interval '1 month');
       m := m + interval '1 month';
     END LOOP;
   END $$;
   ```

2. Seed data: `scripts/seed-orders.sql`
   ```sql
   INSERT INTO orders (id, customer_id, status, total_cents, created_at)
   SELECT gen_random_uuid(),
          ('00000000-0000-0000-0000-' || lpad((random()*10000)::int::text, 12, '0'))::uuid,
          (ARRAY['PENDING','STOCK_RESERVED','CONFIRMED','CANCELLED'])[1 + floor(random()*4)::int],  -- uniform 1..4
          (random()*100000)::bigint,
          now() - (random() * interval '365 days')
   FROM generate_series(1, 1000000);
   ANALYZE orders;
   ```

3. Endpoint `GET /customers/:id/orders?status=CONFIRMED&limit=20` runs:
   ```sql
   SELECT id, status, total_cents, created_at FROM orders
   WHERE customer_id = $1 AND status = $2
   ORDER BY created_at DESC LIMIT 20;
   ```

4. Run `EXPLAIN (ANALYZE, BUFFERS)`, record: node type (`Seq Scan` / `Parallel Seq Scan`), time, buffers.

5. Experiments (record each result in a table in your notes):
   | Index | Expectation |
   |---|---|
   | `(status)` | Planner likely ignores: low selectivity (4 values, ~25% each) |
   | `(created_at)` | Walks index backwards, filters many rows |
   | `(customer_id)` | Bitmap scan + sort |
   | `(customer_id, status, created_at DESC)` | Index scan, no sort, ~ms |
   | `(customer_id, status, created_at DESC) INCLUDE (id, total_cents)` | Index-only scan (after `VACUUM`): every selected column is in the index |
   | Query with only `WHERE status = $2` on the composite index | No index use, except Postgres 18+ skip scan when `customer_id` has few distinct values (not the case here) |

6. Break it on purpose:
   - `WHERE created_at::date = '2026-05-01'` vs range `created_at >= '2026-05-01' AND created_at < '2026-05-02'`.
   - `OFFSET 100000` vs keyset pagination `WHERE (created_at, id) < ($1, $2)`.
   - Mass-update 50% rows, skip `ANALYZE`, observe estimate mismatch; then `ANALYZE`.

7. Build indexes without blocking writes. `CREATE INDEX CONCURRENTLY` is **not supported on a partitioned parent**, so:
   ```sql
   CREATE INDEX idx_orders_cust ON ONLY orders (customer_id, status, created_at DESC) INCLUDE (id, total_cents); -- invalid until all partitions attached
   CREATE INDEX CONCURRENTLY idx_orders_2026_10_cust ON orders_2026_10 (customer_id, status, created_at DESC) INCLUDE (id, total_cents);
   ALTER INDEX idx_orders_cust ATTACH PARTITION idx_orders_2026_10_cust;
   -- repeat per partition (script it); parent index becomes valid when every partition has one
   ```
   On a non-partitioned table, plain `CREATE INDEX CONCURRENTLY` (then check the index is `VALID` in `\d`).

8. Implement keyset pagination in the endpoint (`cursor = base64(created_at|id)`).

**Verify:** query drops from hundreds of ms to < 1–2 ms; `EXPLAIN` shows `Index Scan`/`Index Only Scan` without `Sort`.

**Talking points:** leftmost prefix, equality-then-range, selectivity, stats, covering indexes, why B-Tree here (Postgres) vs LSM for write-heavy event logs.

---

## Step 5. Transactions and Isolation Levels

**Goal:** reproduce lost update and write skew, then fix them.
**Topics:** #3 ACID, #4 Isolation.

### 5.1 Naive reservation (buggy on purpose)
```ts
// inventory.service.ts — WRONG version
async reserveNaive(sku: string, qty: number) {
  return this.ds.transaction(async (m) => {
    const [row] = await m.query(`SELECT qty FROM stock WHERE sku = $1`, [sku]);
    if (row.qty < qty) throw new ConflictException('OUT_OF_STOCK');
    await new Promise((r) => setTimeout(r, 50));            // widen race window
    await m.query(`UPDATE stock SET qty = $1 WHERE sku = $2`, [row.qty - qty, sku]);
  });
}
```

`scripts/race-reserve.ts`: set stock to 10, fire 50 concurrent reservations of 1.
```ts
const results = await Promise.allSettled(
  Array.from({ length: 50 }, () => fetch('http://localhost:3003/stock/A1/reserve-naive', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ qty: 1 }),
  })),
);
```
Expected bug: far more than 10 succeed and final `qty` is wrong (**lost update**).

### 5.2 Four fixes (implement each as a separate endpoint and compare)
1. **Atomic conditional update** (best here):
   ```sql
   UPDATE stock SET qty = qty - $1 WHERE sku = $2 AND qty >= $1;   -- rowCount 0 => out of stock
   ```
2. **Pessimistic lock:** `SELECT qty FROM stock WHERE sku = $1 FOR UPDATE`, then update.
3. **Optimistic lock:** read `version`, `UPDATE ... WHERE sku=$ AND version=$v`; retry on 0 rows (max 5, jitter).
4. **Serializable + retry:**
   ```ts
   async withSerializableRetry<T>(fn: (m: EntityManager) => Promise<T>, attempts = 5): Promise<T> {
     for (let i = 0; ; i++) {
       try {
         return await this.ds.transaction('SERIALIZABLE', fn);
       } catch (e: any) {
         const retryable = e.code === '40001' || e.code === '40P01';
         if (!retryable || i >= attempts - 1) throw e;
         await new Promise((r) => setTimeout(r, Math.random() * 20 * 2 ** i));
       }
     }
   }
   ```

### 5.3 Write skew demo
Invariant: "total reserved for a flash-sale campaign across SKUs `X1` and `X2` ≤ 5".
```sql
CREATE TABLE campaign_reservations (id serial PRIMARY KEY, sku text, qty int);
```
Endpoint: `SELECT sum(qty) FROM campaign_reservations` → if `< 5` → `INSERT` one row. Run concurrently under `REPEATABLE READ`: sum ends > 5 (write skew: no row was updated twice). Then:
- Run under `SERIALIZABLE` + retry: invariant holds, some requests get `40001` and retry.
- Alternative: lock a single `campaigns` row `FOR UPDATE` (materialize the conflict).

### 5.4 Dual-write awareness
Add a comment/test showing that `publish()` inside `transaction()` is not atomic; fixed in Step 11.

**Verify:** a table in your notes: approach × (correct?, throughput, p99, error/retry count) from running the race script.

**Talking points:** Read Committed default; atomic SQL beats read-modify-write; Postgres RR = snapshot isolation allows write skew; SSI needs retries; keep transactions short, no HTTP calls inside.

---

## Step 6. Replication and Read/Write Split

**Goal:** writes to primary, reads to replica, observe lag, implement read-your-writes.
**Topics:** #5 Replication.

1. TypeORM supports replication natively:
   ```ts
   TypeOrmModule.forRoot({
     type: 'postgres',
     replication: {
       master: { url: process.env.DB_URL },
       slaves: [{ url: process.env.DB_REPLICA_URL }],
     },
     autoLoadEntities: true,
   });
   ```
   Queries via `find`/`QueryBuilder` outside a transaction go to the replica; writes and transactions go to the primary.

2. **Show lag:** on the replica run `SELECT now() - pg_last_xact_replay_timestamp();`. Induce lag **without freezing the replica**: on the replica run `SELECT pg_wal_replay_pause();`, write a few orders on the primary, read them via the replica (stale: they are missing), then `SELECT pg_wal_replay_resume();`.
   > Do not use `docker compose pause pg-replica` for this: a paused container does not answer at all, so reads hang/time out instead of returning stale data, and you demonstrate an outage, not lag.

3. **Read-your-writes:** after a client creates/updates an order, return header `X-Read-Primary-Until: <now+5s>` or keep it server-side: store `rw:<customerId> = 1` in Redis with 5 s TTL; `OrdersRepository.findForCustomer` checks it and uses a primary `QueryRunner` (`ds.createQueryRunner('master')`) if set.
   Alternative (more precise): after commit read `pg_current_wal_lsn()`; reader waits until replica `pg_last_wal_replay_lsn() >= lsn` (with timeout), else falls back to primary.

4. **Durability experiment:** on the primary:
   ```sql
   ALTER SYSTEM SET synchronous_standby_names = '*';   -- any connected standby must confirm
   ALTER SYSTEM SET synchronous_commit = 'on';
   SELECT pg_reload_conf();
   SELECT application_name, sync_state FROM pg_stat_replication;   -- expect 'sync'
   ```
   Measure insert latency with k6 before/after. Then `docker compose stop pg-replica` and observe writes **hang** (sync replication blocks without a standby). Restore async: `ALTER SYSTEM RESET synchronous_standby_names; SELECT pg_reload_conf();`.

5. **Failover (manual):** stop primary, promote replica (`pg_ctl promote` or `SELECT pg_promote();`), point `DB_URL` to it. Discuss: writes acknowledged before stop but not replicated (async) are lost; in production use Patroni + etcd for automatic failover with fencing.

**Verify:** log which node served each query (`SELECT pg_is_in_recovery()`: `true` on the replica); read-after-write test passes while WAL replay is paused.

**Talking points:** async vs sync vs semi-sync, lag anomalies, failover risks, split brain, replication ≠ backup.

---

## Step 7. Partitioning and Sharding

**Goal:** range-partition `orders` by month; simulate hash sharding of customers across two databases.
**Topics:** #7 Sharding/partitioning.

### 7.1 Declarative partitioning
Monthly partitions already exist from Step 4. Add an (ideally always empty) safety net:
```sql
CREATE TABLE orders_default PARTITION OF orders DEFAULT;
```
- Scheduled job (Nest `@nestjs/schedule` cron) creates next month's partition **in advance** (same `DO` block as Step 4).
- **DEFAULT partition gotcha:** if rows for a month landed in `orders_default` (job did not run), `CREATE TABLE orders_2027_01 PARTITION OF orders ...` fails with *"updated partition constraint for default partition would be violated"*. Fix:
  ```sql
  BEGIN;
  ALTER TABLE orders DETACH PARTITION orders_default;
  CREATE TABLE orders_2027_01 PARTITION OF orders FOR VALUES FROM ('2027-01-01') TO ('2027-02-01');
  INSERT INTO orders SELECT * FROM orders_default WHERE created_at >= '2027-01-01' AND created_at < '2027-02-01';
  DELETE FROM orders_default WHERE created_at >= '2027-01-01' AND created_at < '2027-02-01';
  ALTER TABLE orders ATTACH PARTITION orders_default DEFAULT;
  COMMIT;
  ```
  Alert when `orders_default` is not empty.
- Retention: `ALTER TABLE orders DETACH PARTITION orders_2025_09; DROP TABLE orders_2025_09;`
- `EXPLAIN` a query with `created_at` range: only relevant partitions scanned (**partition pruning**). Without `created_at` in `WHERE`, all partitions are scanned; discuss.

### 7.2 Application-level sharding (simulation)
Add two more Postgres containers `pg-shard-0`, `pg-shard-1` (or two databases `orders_s0`, `orders_s1`).

`libs/common/sharding/hash-ring.ts`: consistent hashing ring with virtual nodes (code in theory doc, topic 7).

`ShardRouter`:
```ts
@Injectable()
export class ShardRouter {
  private ring = new Ring(['s0', 's1'], 128);
  constructor(@Inject('SHARDS') private shards: Record<string, DataSource>) {}
  forCustomer(customerId: string): DataSource {
    return this.shards[this.ring.get(customerId)];
  }
}
```
- `POST /orders` and `GET /customers/:id/orders` hit one shard (shard key = `customer_id`).
- `GET /orders?status=CONFIRMED` (admin) is **scatter-gather**: query all shards in parallel, merge-sort, limit.
- Experiment: add `s2` to the ring, count how many of 10k customer IDs move (~1/3) vs `hash % N` (~2/3).

**Verify:** script prints distribution per shard and moved-keys percentage.

**Talking points:** shard key choice, hot keys, cross-shard queries/transactions, resharding difficulty, why partitioning on one node is not sharding.

---

## Step 8. Consensus and CAP (Experiment)

**Goal:** see Raft leader election in a real system; classify our components by CAP/PACELC.
**Topics:** #6 Raft, #8 CAP/PACELC.

Do **not** implement Raft in the app. Run a 3-node etcd cluster:

`infra/etcd/docker-compose.etcd.yml`:
```yaml
x-etcd: &etcd
  image: quay.io/coreos/etcd:v3.5.15
services:
  etcd1:
    <<: *etcd
    command: >
      etcd --name etcd1 --initial-advertise-peer-urls http://etcd1:2380
      --listen-peer-urls http://0.0.0.0:2380 --listen-client-urls http://0.0.0.0:2379
      --advertise-client-urls http://etcd1:2379
      --initial-cluster etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380
      --initial-cluster-state new
  etcd2:
    <<: *etcd
    command: >
      etcd --name etcd2 --initial-advertise-peer-urls http://etcd2:2380
      --listen-peer-urls http://0.0.0.0:2380 --listen-client-urls http://0.0.0.0:2379
      --advertise-client-urls http://etcd2:2379
      --initial-cluster etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380
      --initial-cluster-state new
  etcd3:
    <<: *etcd
    command: >
      etcd --name etcd3 --initial-advertise-peer-urls http://etcd3:2380
      --listen-peer-urls http://0.0.0.0:2380 --listen-client-urls http://0.0.0.0:2379
      --advertise-client-urls http://etcd3:2379
      --initial-cluster etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380
      --initial-cluster-state new
```

Experiments:
1. `docker exec etcd1 etcdctl --endpoints=etcd1:2379,etcd2:2379,etcd3:2379 endpoint status -w table` shows leader and **raft term**.
2. Stop the leader container; re-run status: new leader, term incremented. Writes (`etcdctl put k v`) still work (2 of 3 = majority).
3. Stop a second node: writes fail/time out (no majority) → **CP** behavior. Reads with `--consistency=s` (serializable, local) may still return possibly stale data; linearizable reads fail.
4. Optional: leader election for our `outbox-relay` using an etcd lease (`etcd3` npm package `Election`) so only one relay instance runs at a time. Discuss fencing tokens (lease revision).

Write `docs/cap-classification.md` (short table):
| Component / operation | During partition | Normal (PACELC) |
|---|---|---|
| Payment write (primary only) | C: unavailable if primary unreachable | EC |
| Order reads from replica | A: may be stale | EL |
| Redis cache / rate limits | A: async replication, may lose writes | EL |
| RabbitMQ quorum queues | C: minority side cannot accept | EC |
| etcd | C | EC |

**Talking points:** majority quorum, odd node count, randomized timeouts, terms, "CA" is not a real option, per-operation choice.

---

## Step 9. Caching with Redis

**Goal:** cache-aside for order reads, with protection from stampede, avalanche, penetration, stale races.
**Topics:** #9 Caching, #10 Cache problems.

`libs/common/cache/cache.service.ts`:
```ts
@Injectable()
export class CacheService {
  private readonly logger = new Logger(CacheService.name);
  private inflight = new Map<string, Promise<unknown>>();
  constructor(@Inject('REDIS') private redis: Redis) {}

  private ttlWithJitter(base: number) {
    return base + Math.floor(Math.random() * base * 0.1);          // avalanche protection
  }

  async getOrLoad<T>(key: string, ttlSec: number, loader: () => Promise<T | null>): Promise<T | null> {
    const raw = await this.redis.get(key);
    if (raw !== null) {
      const entry = JSON.parse(raw) as { v: T | null; softExp: number };
      if (Date.now() > entry.softExp) {
        // stale-while-revalidate; a fire-and-forget promise must handle its own rejection,
        // otherwise Node crashes on unhandledRejection
        this.refresh(key, ttlSec, loader).catch((e) => this.logger.warn(`refresh ${key} failed: ${e}`));
      }
      return entry.v;
    }
    return this.singleFlight(key, () => this.loadWithLock(key, ttlSec, loader));
  }

  // in-process request coalescing
  private singleFlight<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const existing = this.inflight.get(key) as Promise<T> | undefined;
    if (existing) return existing;
    const p = fn().finally(() => this.inflight.delete(key));
    this.inflight.set(key, p);
    return p;
  }

  // cross-instance stampede protection
  private async loadWithLock<T>(key: string, ttlSec: number, loader: () => Promise<T | null>) {
    const lockKey = `lock:${key}`;
    const token = randomUUID();                                     // identifies the lock owner
    const gotLock = await this.redis.set(lockKey, token, 'PX', 5000, 'NX');
    if (!gotLock) {
      for (let i = 0; i < 10; i++) {
        await new Promise((r) => setTimeout(r, 50));
        const raw = await this.redis.get(key);
        if (raw !== null) return (JSON.parse(raw) as { v: T | null }).v;
      }
      return loader();                                              // give up waiting
    }
    try {
      const v = await loader();
      await this.write(key, v, v === null ? 30 : ttlSec);           // negative caching, short TTL
      return v;
    } finally {
      // compare-and-delete: if the loader outlived the 5s TTL, another instance may own the lock now
      await this.redis.eval(
        `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end`,
        1, lockKey, token);
    }
  }

  private async refresh<T>(key: string, ttlSec: number, loader: () => Promise<T | null>) {
    const ok = await this.redis.set(`refresh:${key}`, '1', 'PX', 5000, 'NX');
    if (!ok) return;
    const v = await loader();
    await this.write(key, v, ttlSec);
  }

  private async write<T>(key: string, v: T | null, ttlSec: number) {
    const hard = this.ttlWithJitter(ttlSec * 2);
    const softExp = Date.now() + ttlSec * 1000;
    await this.redis.set(key, JSON.stringify({ v, softExp }), 'EX', hard);
  }

  async invalidate(key: string) {
    await this.redis.del(key);
    setTimeout(() => void this.redis.del(key), 1000);               // delayed double delete
  }
}
```

Usage in `order-service`:
```ts
getOrder(id: string) {
  return this.cache.getOrLoad(`order:${id}`, 60, () => this.repo.findOneFromPrimary(id));
}
```
- Populate cache **from the primary** to avoid caching replica-stale data.
- Invalidate **after commit**, driven by the `OrderStatusChanged` event (Step 11) so invalidation survives crashes.

Experiments:
1. Hit ratio: k6 with Zipf-like ID distribution; read `INFO stats` (`keyspace_hits/misses`).
2. Stampede: disable lock + single-flight, set TTL 2 s, 500 VUs on one ID; count DB queries (`pg_stat_statements`). Re-enable; compare.
3. Penetration: request 10k random non-existent IDs with and without negative caching.
4. Stale race: add artificial delay between DB read and `SET`; update order in between; show stale value; show double-delete/short TTL mitigation.
5. Redis down: `docker compose stop redis`; make `CacheService` fail-open (catch errors → call loader) and see DB load jump. Discuss sizing.
6. Write-behind counter (stale-counter trade-off): add `view_count bigint NOT NULL DEFAULT 0` to `orders`. Each `GET /orders/:id` does `INCR views:order:<id>` (no DB write). A cron every 10 s scans `views:order:*` with `SCAN`, takes each value with `GETDEL` (atomic read-and-reset, Redis 6.2+) and runs `UPDATE orders SET view_count = view_count + $n WHERE id = $id`. Observe: DB writes drop from one per view to one per key per 10 s; the DB value lags up to 10 s; if Redis dies before the flush, those increments are lost. Acceptable for view counters, never for money.

**Talking points:** cache-aside vs write-through, delete not set, TTL jitter, single-flight + lock, negative cache, cold cache risk.

---

## Step 10. RabbitMQ Messaging

**Goal:** reliable publish/consume with acks, prefetch, retries with delay, DLQ.
**Topics:** #11 Queues and brokers.

Routing keys (one table, used by every service; `routingKeyFor(type)` maps type → key):

| Exchange | Routing key | Message type | Sent by |
|---|---|---|---|
| `orders.commands` | `inventory.reserve` | `ReserveStock` | order (saga) |
| `orders.commands` | `inventory.release` | `ReleaseStock` | order (saga) |
| `orders.commands` | `payment.charge` | `ChargePayment` | order (saga) |
| `orders.events` | `order.created` / `order.confirmed` / `order.cancelled` | `OrderCreated` / `OrderConfirmed` / `OrderCancelled` | order |
| `orders.events` | `inventory.stock-reserved` / `inventory.stock-rejected` / `inventory.stock-released` | `StockReserved` / `StockRejected` / `StockReleased` | inventory |
| `orders.events` | `payment.succeeded` / `payment.failed` / `payment.deferred` | `PaymentSucceeded` / `PaymentFailed` / `PaymentDeferred` | payment |

Topology (declared on startup by `libs/common/messaging/topology.ts`):
```
exchange  orders.events   (topic, durable)
exchange  orders.commands (topic, durable)     # topic, not direct: direct exchanges ignore wildcards
exchange  dlx             (direct, durable)

queue inventory.commands        bind orders.commands  rk=inventory.*
queue payment.commands          bind orders.commands  rk=payment.*
queue order.saga-replies        bind orders.events    rk=inventory.#  and  rk=payment.#
queue notification.order-events bind orders.events    rk=order.#

every main queue <name> args:
  x-queue-type=quorum
  x-dead-letter-exchange=dlx
  x-dead-letter-routing-key=<name>   # otherwise the original key (e.g. inventory.reserve) is kept and matches no DLQ binding
  x-delivery-limit=10                # quorum queues: dead-letter after 10 redeliveries (crash loops, requeue=true nacks)

queue <name>.retry   args: x-message-ttl=5000, x-dead-letter-exchange="" (default exchange), x-dead-letter-routing-key=<name>
                     # the message comes back to <name> directly; routing through the original exchange
                     # would use routing key "<name>.retry", which no binding matches
queue <name>.dlq     bind dlx rk=<name>
```

```ts
export async function declareQueue(ch: ConfirmChannel, name: string, exchange: string, keys: string[]) {
  await ch.assertQueue(name, { durable: true, arguments: {
    'x-queue-type': 'quorum',
    'x-dead-letter-exchange': 'dlx',
    'x-dead-letter-routing-key': name,
    'x-delivery-limit': 10,
  } });
  for (const k of keys) await ch.bindQueue(name, exchange, k);

  await ch.assertQueue(`${name}.retry`, { durable: true, arguments: {
    'x-message-ttl': 5000,
    'x-dead-letter-exchange': '',
    'x-dead-letter-routing-key': name,
  } });

  await ch.assertQueue(`${name}.dlq`, { durable: true });
  await ch.bindQueue(`${name}.dlq`, 'dlx', name);
}
```

Publisher with confirms (`amqp-connection-manager`):
```ts
const conn = amqp.connect([process.env.AMQP_URL!]);
const channel = conn.createChannel({
  json: true,
  confirm: true,                                     // publisher confirms
  setup: (ch: ConfirmChannel) => declareTopology(ch),
});

export async function publish(exchange: string, routingKey: string, msg: EventEnvelope) {
  await channel.publish(exchange, routingKey, msg, {
    persistent: true,
    messageId: msg.id,
    headers: { 'x-correlation-id': msg.correlationId },
  });                                                // resolves after broker confirm
}
```

Event envelope (`libs/common/messaging/envelope.ts`):
```ts
export interface EventEnvelope<T = unknown> {
  id: string;            // = outbox id, used for dedup
  type: string;          // 'OrderCreated'
  aggregateId: string;   // order id
  version: number;       // aggregate version, for ordering checks
  occurredAt: string;
  correlationId: string;
  payload: T;
}
```

Consumer base with manual ack, retries, DLQ:
```ts
channel.addSetup(async (ch: ConfirmChannel) => {
  await ch.prefetch(10);
  await ch.consume(queue, async (raw) => {
    if (!raw) return;
    const msg = JSON.parse(raw.content.toString()) as EventEnvelope;
    const attempt = Number(raw.properties.headers?.['x-attempt'] ?? 0);
    try {
      await handler(msg);                                   // idempotent (Step 11 inbox)
      ch.ack(raw);
    } catch (e) {
      if (attempt >= 5 || isPoison(e)) {
        ch.nack(raw, false, false);                         // -> dlx (rk=<queue>) -> <queue>.dlq
        return;
      }
      try {
        // wait for the broker to confirm the retry copy BEFORE acking the original;
        // acking first could lose the message if the broker fails in between
        await new Promise<void>((resolve, reject) =>
          ch.sendToQueue(`${queue}.retry`, raw.content, {
            persistent: true, messageId: raw.properties.messageId,
            headers: { ...raw.properties.headers, 'x-attempt': attempt + 1 },
          }, (err) => (err ? reject(err) : resolve())));
        ch.ack(raw);
      } catch {
        ch.nack(raw, false, true);                          // could not schedule retry: requeue now
      }
    }
  });
});
```
> Fixed TTL retry queue gives constant delay. For exponential delay, use several retry queues (`.retry.1s`, `.retry.5s`, `.retry.30s`) or the `rabbitmq_delayed_message_exchange` plugin.
>
> Two safety nets for poison messages: the `x-attempt` counter (handler threw) and `x-delivery-limit` on the quorum queue (consumer crashed before acking, so the counter never incremented).

Experiments:
1. Kill the consumer mid-processing (no ack): message is redelivered (`redelivered=true`).
2. Throw always (poison): ends in DLQ after 5 attempts; inspect in UI; write a small "replay DLQ" script.
3. Crash the process inside the handler every time (`process.exit(1)`): after 10 redeliveries `x-delivery-limit` dead-letters it.
4. Change prefetch 1 vs 100 with slow handler and 3 consumers: observe fairness.
5. Watch queue depth and consumer utilization in the management UI while the consumer is slow (the RabbitMQ equivalent of Kafka **consumer lag**).

### 10.1 Event stream practice (Kafka concepts on RabbitMQ Streams)
The curriculum names Kafka. RabbitMQ Streams give the same log semantics (append-only, retention, offset-based replay, independent readers) on the broker you already run.

```ts
await ch.assertQueue('orders.stream', { durable: true, arguments: {
  'x-queue-type': 'stream',
  'x-max-age': '7D',                       // retention, like Kafka retention.ms
} });
await ch.bindQueue('orders.stream', 'orders.events', 'order.#');

// a stream consumer needs prefetch and manual ack; it chooses where to start reading
await ch.prefetch(100);
await ch.consume('orders.stream', (m) => {
  if (!m) return;
  const offset = m.properties.headers?.['x-stream-offset'];  // position in the log
  analytics.record(JSON.parse(m.content.toString()), offset);
  ch.ack(m);                                                 // flow control only; does NOT delete the message
}, { arguments: { 'x-stream-offset': 'first' } });           // 'first' | 'last' | 'next' | <number> | <Date>
```

Exercises:
1. Start an `analytics` reader from `first` after a day of orders: it replays the full history (impossible with a classic queue).
2. Run two independent readers (`analytics`, `audit`): each gets every event, like two Kafka consumer groups.
3. Persist the last processed offset in Postgres and resume from it after restart (Kafka's offset commit; commit **after** processing = at-least-once).
4. Compare: ordering is per stream (Kafka: per partition); scaling consumers of one stream in parallel needs super streams (RabbitMQ's partitioned streams ≈ Kafka partitions with a routing key).

Optional, real Kafka: add a single-node Redpanda or Kafka (KRaft) container, produce `order.*` events from the outbox relay with key = `orderId`, and consume with a consumer group. Node clients: `@confluentinc/kafka-javascript` (maintained; has a KafkaJS-compatible API). `kafkajs` itself is no longer actively maintained.

**Talking points:** queue vs log, at-least-once default, ordering per aggregate, poison messages, publisher confirms, quorum queues, replay, consumer lag.

---

## Step 11. Idempotency, Outbox, Inbox

**Goal:** never charge twice, never lose an event.
**Topics:** #12 Idempotency and transactional outbox.

### 11.1 Idempotency keys for `POST /payments`
Table from theory doc (`idempotency_keys`, primary key `(user_id, key)` so two clients cannot collide on the same key). Interceptor in `payment-service`:
```ts
const STALE_LOCK = '30 seconds';   // an IN_PROGRESS row older than this belongs to a crashed request

@Injectable()
export class IdempotencyInterceptor implements NestInterceptor {
  constructor(private ds: DataSource) {}

  async intercept(ctx: ExecutionContext, next: CallHandler): Promise<Observable<unknown>> {
    const req = ctx.switchToHttp().getRequest();
    const key = req.headers['idempotency-key'];
    if (!key) throw new BadRequestException('Idempotency-Key header required');
    const userId = req.user?.id ?? req.headers['x-api-key'] ?? 'anonymous';   // keys are scoped per client
    const hash = createHash('sha256').update(JSON.stringify(req.body)).digest('hex');

    // claim the key: insert a new row, or take over a stale IN_PROGRESS row with the same payload
    const claimed = await this.ds.query(
      `INSERT INTO idempotency_keys (user_id, key, request_hash, status, locked_at)
       VALUES ($1, $2, $3, 'IN_PROGRESS', now())
       ON CONFLICT (user_id, key) DO UPDATE SET locked_at = now()
         WHERE idempotency_keys.status = 'IN_PROGRESS'
           AND idempotency_keys.locked_at < now() - interval '${STALE_LOCK}'
           AND idempotency_keys.request_hash = EXCLUDED.request_hash
       RETURNING key`, [userId, key, hash]);

    if (claimed.length === 0) {
      const [row] = await this.ds.query(
        `SELECT * FROM idempotency_keys WHERE user_id = $1 AND key = $2`, [userId, key]);
      if (row.request_hash !== hash) throw new UnprocessableEntityException('Key reused with different payload');
      if (row.status === 'IN_PROGRESS') throw new ConflictException('Request in progress');
      return of(row.response_body);                           // replay stored response
    }

    return next.handle().pipe(
      mergeMap(async (body) => {
        await this.ds.query(
          `UPDATE idempotency_keys SET status = 'COMPLETED', response_body = $3
           WHERE user_id = $1 AND key = $2`, [userId, key, JSON.stringify(body)]);
        return body;
      }),
      catchError(async (err) => {
        // allow the client to retry; safe because the provider call is itself idempotent (same key)
        await this.ds.query(`DELETE FROM idempotency_keys WHERE user_id = $1 AND key = $2`, [userId, key]);
        throw err;
      }),
    );
  }
}
```
> **Status code on replay:** Nest applies the handler's status code (`@HttpCode(...)`, default 201 for POST) when it writes the response, *after* interceptors run. So `res.status(...)` set inside an interceptor is overwritten, and `res.statusCode` read in `mergeMap` is not final yet. That is why only the body is stored: a replay returns the same body with the handler's fixed code. Model business outcomes (e.g. card declined) in the body (`status: 'FAILED'`), not as thrown errors, so they are stored and replayed too. If you need per-request codes, inject `@Res({ passthrough: true })` in the handler and store the code it sets.
- Pass the same key to `fake-payment-provider` (`Idempotency-Key` header); provider stores processed keys and returns the same charge.
- Cleanup cron: delete keys older than 24 h.
- Stronger variant: write payment row + mark key completed in **one** transaction.

### 11.2 Transactional outbox
`libs/common/outbox/outbox.entity.ts` → `outbox` table (theory doc) plus two columns, `exchange text NOT NULL` and `routing_key text NOT NULL`, because the same outbox carries both events (`orders.events`) and saga commands (`orders.commands`). Map entity properties to snake_case columns (`@Column({ name: 'aggregate_id' })` or a `SnakeNamingStrategy`). Business code:
```ts
await this.ds.transaction(async (m) => {
  const order = await m.save(Order, newOrder);
  await m.insert(Outbox, {
    id: randomUUID(), aggregateType: 'order', aggregateId: order.id,
    eventType: 'OrderCreated', exchange: 'orders.events', routingKey: 'order.created',
    payload: { ...order, items },
  });
});                                     // no broker call here
```

Relay (`libs/common/outbox/outbox-relay.service.ts`), runs every 200 ms:
```ts
async relayBatch() {
  await this.ds.transaction(async (m) => {
    // the transaction holds row locks while publishing, so bound how long it can live
    await m.query(`SET LOCAL idle_in_transaction_session_timeout = '10s'`);
    const rows: OutboxRow[] = await m.query(`
      SELECT * FROM outbox WHERE published_at IS NULL
      ORDER BY created_at LIMIT 20 FOR UPDATE SKIP LOCKED`);       // small batch = short transaction
    for (const r of rows) {
      // a hung broker must not keep the transaction (and its locks) open forever
      await withTimeout(this.publisher.publish(r.exchange, r.routing_key, toEnvelope(r)), 2000, 'publish');
      await m.query(`UPDATE outbox SET published_at = now() WHERE id = $1`, [r.id]);
    }
  });
}
```
- On publish timeout the transaction rolls back; rows stay unpublished and are retried on the next tick (a message that *did* reach the broker is published again → inbox dedups).
- Crash after publish but before commit → republished → duplicates handled by inbox.
- Multiple relay instances are safe thanks to `SKIP LOCKED` (or one leader via etcd from Step 8).
- Cleanup: delete published rows older than 7 days.

### 11.3 Inbox on consumers
```sql
CREATE TABLE inbox (
  message_id   uuid NOT NULL,
  handler      text NOT NULL,                 -- one message may be handled by several handlers in a service
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (message_id, handler)
);
```
```ts
async handleOnce(msg: EventEnvelope, handlerName: string, fn: (m: EntityManager) => Promise<void>) {
  await this.ds.transaction(async (m) => {
    const res = await m.query(
      `INSERT INTO inbox (message_id, handler) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING message_id`,
      [msg.id, handlerName]);
    if (res.length === 0) return;      // duplicate -> skip, caller acks
    await fn(m);                       // business effect in the same transaction
  });
}
```

Experiments:
1. Send `POST /payments` twice with the same key, and 20 concurrent with the same key: exactly one charge.
2. Kill `order-service` right after commit (`process.exit` hook in test mode): event still published by relay after restart.
3. Kill relay between publish and update: consumer receives duplicate, inbox skips it.

**Talking points:** dual-write problem, at-least-once + idempotent consumer = effectively once, keys passed to external providers, polling vs CDC (Debezium).

---

## Step 12. Saga for Order Creation

**Goal:** orchestrated saga across order, inventory, payment with compensations.
**Topics:** #13 Distributed transactions.

Flow:
```mermaid
sequenceDiagram
  participant C as Client
  participant O as Order (orchestrator)
  participant I as Inventory
  participant P as Payment
  C->>O: POST /orders (Idempotency-Key)
  O->>O: tx: order PENDING + saga row + outbox(ReserveStock)
  O-->>C: 202 Accepted {orderId}
  O->>I: ReserveStock (via outbox/RabbitMQ)
  I-->>O: StockReserved | StockRejected
  O->>P: ChargePayment
  P-->>O: PaymentSucceeded | PaymentFailed
  alt payment failed
    O->>I: ReleaseStock (compensation)
    O->>O: order CANCELLED
  else success
    O->>O: order CONFIRMED, outbox(OrderConfirmed)
  end
```

Saga state table:
```sql
CREATE TABLE order_sagas (
  order_id    uuid PRIMARY KEY,
  state       text NOT NULL,       -- STARTED | STOCK_RESERVED | PAYMENT_DEFERRED | CONFIRMED | COMPENSATING | CANCELLED
  step_deadline timestamptz,       -- timeout for current step; NULL in terminal states
  step_attempts int NOT NULL DEFAULT 0,
  version     int NOT NULL DEFAULT 0,
  updated_at  timestamptz NOT NULL DEFAULT now()
);
```
Created in the same transaction as the order: `INSERT INTO order_sagas (order_id, state, step_deadline) VALUES ($1, 'STARTED', now() + interval '30 seconds')` plus outbox `ReserveStock`. Order status equals the saga state, except `STARTED`, which is shown as `PENDING` (see the CHECK constraint in Step 3).

Orchestrator (one transition table; each transition = one local transaction: saga row + order row + outbox):
```ts
type Msg = { exchange: string; routingKey: string; type: string };
const cmd = (routingKey: string, type: string): Msg => ({ exchange: 'orders.commands', routingKey, type });
const evt = (routingKey: string, type: string): Msg => ({ exchange: 'orders.events', routingKey, type });

// current state -> reply type -> [next state, messages to send through the outbox]
const SAGA: Record<string, Record<string, [string, Msg[]]>> = {
  STARTED: {
    StockReserved:    ['STOCK_RESERVED',   [cmd('payment.charge', 'ChargePayment')]],
    StockRejected:    ['CANCELLED',        [evt('order.cancelled', 'OrderCancelled')]],
  },
  STOCK_RESERVED: {
    PaymentSucceeded: ['CONFIRMED',        [evt('order.confirmed', 'OrderConfirmed')]],
    PaymentFailed:    ['COMPENSATING',     [cmd('inventory.release', 'ReleaseStock')]],
    PaymentDeferred:  ['PAYMENT_DEFERRED', []],              // provider down, breaker open (Step 14)
  },
  PAYMENT_DEFERRED: {
    PaymentSucceeded: ['CONFIRMED',        [evt('order.confirmed', 'OrderConfirmed')]],
    PaymentFailed:    ['COMPENSATING',     [cmd('inventory.release', 'ReleaseStock')]],
  },
  COMPENSATING: {
    StockReleased:    ['CANCELLED',        [evt('order.cancelled', 'OrderCancelled')]],
  },
};
const TERMINAL = new Set(['CONFIRMED', 'CANCELLED']);
const STEP_TIMEOUT = { PAYMENT_DEFERRED: '10 minutes', default: '30 seconds' };

async onReply(reply: EventEnvelope) {
  await this.inbox.handleOnce(reply, 'order-saga', async (m) => {
    const [s] = await m.query(`SELECT * FROM order_sagas WHERE order_id = $1 FOR UPDATE`, [reply.aggregateId]);
    const transition = SAGA[s.state]?.[reply.type];
    if (!transition) return;                     // late, duplicate or out-of-order reply: ignore
    const [to, messages] = transition;
    const timeout = STEP_TIMEOUT[to as keyof typeof STEP_TIMEOUT] ?? STEP_TIMEOUT.default;
    await m.query(
      `UPDATE order_sagas
         SET state = $2, version = version + 1, updated_at = now(), step_attempts = 0,
             step_deadline = CASE WHEN $3 THEN NULL ELSE now() + $4::interval END
       WHERE order_id = $1`, [s.order_id, to, TERMINAL.has(to), timeout]);
    await m.query(`UPDATE orders SET status = $2, updated_at = now() WHERE id = $1`, [s.order_id, to]);
    for (const msg of messages) await this.outbox.add(m, s.order_id, msg);
  });
}
```

Participant rules:
- `inventory.reserve`: atomic conditional update (Step 5) + `reservations` row; idempotent by `(order_id, sku)` PK. Replies `StockReserved` / `StockRejected`.
- `inventory.release`: set reservation `RELEASED` and return qty only if it was `RESERVED` (idempotent). Replies `StockReleased` (also when already released).
- `payment.charge`: uses `order_id` UNIQUE + provider idempotency key = `order_id`. Replies `PaymentSucceeded` / `PaymentFailed`, or `PaymentDeferred` when the circuit breaker is open (Step 14); deferred payments are retried by a payment-service job.
- Payment is the **pivot**: after success, confirmation must eventually succeed (retry forever with alerting).

Timeouts: cron every 5 s selects `FROM order_sagas WHERE step_deadline < now() FOR UPDATE SKIP LOCKED`:
- `STARTED` / `COMPENSATING`: resend the current command (idempotent), increment `step_attempts`, push `step_deadline`; after N attempts, alert (stock commands should not fail permanently).
- `STOCK_RESERVED` / `PAYMENT_DEFERRED` (payment outcome unknown): **never compensate blindly**. A timed-out charge may have succeeded at the provider. First call payment-service `POST /payments/{orderId}/resolve` (it checks the provider by idempotency key and, if no payment row exists yet, inserts a `FAILED` tombstone row so the `UNIQUE (order_id)` constraint blocks a late `ChargePayment` still sitting in the queue):
  - `SUCCEEDED` → apply the `PaymentSucceeded` transition;
  - `FAILED` (including the tombstone) → apply `PaymentFailed` (release stock);
  - still `PENDING` / payment-service unreachable → extend the deadline and ask again later.
  Releasing stock for an order that was actually charged gives a paid order with no goods.

Choreography comparison (optional exercise): implement the same flow with services reacting to each other's events, no `order_sagas`. Note how the flow becomes implicit.

**Verify:**
- Happy path: order becomes `CONFIRMED`.
- Payment provider set to fail 100%: order `CANCELLED`, stock restored.
- Kill `order-service` mid-saga: resumes after restart.
- Duplicate replies: no double transition.

**Talking points:** why not 2PC, compensations are semantic, pivot step, semantic lock (`PENDING`), idempotent steps, persisted state + timeouts, orchestration vs choreography, TCC.

---

## Step 13. Timeouts, Retries, Backoff

**Goal:** resilient HTTP client with deadlines and safe retries.
**Topics:** #14 Retries, timeout, backoff.

`fake-payment-provider` failure injection (env or admin endpoint `POST /chaos`):
```ts
@Post('charges')
async charge(@Headers('idempotency-key') key: string, @Body() body: ChargeDto) {
  const { errorRate, latencyMs, mode } = this.chaos.get();   // mode: ok | slow | down
  if (mode === 'down') throw new ServiceUnavailableException();
  await sleep(latencyMs + Math.random() * latencyMs);
  if (Math.random() < errorRate) throw new BadGatewayException();
  return this.store.chargeOnce(key, body);                    // idempotent by key
}
```

Retry budget (token bucket: each success adds 0.1 token, each retry costs 1, max 10) keeps retries ≤ ~10% of traffic. Create **one instance per dependency** (a shared budget would let one failing dependency starve retries to healthy ones):
```ts
export class RetryBudget {
  private tokens = 10;
  onSuccess() { this.tokens = Math.min(10, this.tokens + 0.1); }
  tryAcquire() { if (this.tokens >= 1) { this.tokens -= 1; return true; } return false; }
}

export const providerBudget = new RetryBudget();   // payment provider
```

`libs/common/http/resilient-http.ts`:
```ts
export interface CallOptions {
  timeoutMs: number;
  retries: number;          // extra attempts
  baseDelayMs?: number;
  capDelayMs?: number;
  idempotent: boolean;      // only retry if true (or idempotency key present)
  deadline?: number;        // absolute epoch ms propagated from the gateway
  budget: RetryBudget;      // per-dependency retry budget
}

export async function resilientCall<T>(fn: (signal: AbortSignal) => Promise<T>, o: CallOptions): Promise<T> {
  const base = o.baseDelayMs ?? 100, cap = o.capDelayMs ?? 2000;
  for (let attempt = 0; ; attempt++) {
    const remaining = o.deadline ? o.deadline - Date.now() : Infinity;
    if (remaining <= 0) throw new GatewayTimeoutException('deadline exceeded');
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), Math.min(o.timeoutMs, remaining));
    try {
      const result = await fn(ctrl.signal);
      o.budget.onSuccess();                                               // refill the budget
      return result;
    } catch (e) {
      const canRetry = o.idempotent && attempt < o.retries && isRetryable(e) && o.budget.tryAcquire();
      if (!canRetry) throw e;
      const backoff = Math.random() * Math.min(cap, base * 2 ** attempt);  // full jitter
      const delay = Math.max(backoff, retryAfterMs(e));                   // server's Retry-After wins
      if (delay >= remaining) throw e;                                    // retry cannot finish within the deadline
      await sleep(delay);
    } finally {
      clearTimeout(t);
    }
  }
}

function isRetryable(e: any): boolean {
  // fetch abort -> AbortError; axios abort -> CanceledError (code ERR_CANCELED);
  // axios own timeout -> ECONNABORTED / ETIMEDOUT
  if (e?.name === 'AbortError' || e?.name === 'CanceledError') return true;
  if (['ERR_CANCELED', 'ECONNABORTED', 'ETIMEDOUT', 'ECONNRESET', 'ECONNREFUSED'].includes(e?.code)) return true;
  const s = e?.response?.status;
  return s === 502 || s === 503 || s === 504 || s === 429;
}

function retryAfterMs(e: any): number {
  const h = e?.response?.headers?.['retry-after'];
  if (!h) return 0;
  const secs = Number(h);
  if (Number.isFinite(secs)) return secs * 1000;          // "Retry-After: 2"
  const at = Date.parse(h);                               // "Retry-After: <HTTP date>"
  return Number.isNaN(at) ? 0 : Math.max(0, at - Date.now());
}
```
With axios, pass the signal: `axios.post(url, body, { signal, headers: { 'Idempotency-Key': key } })`.

Deadline propagation: gateway sets `X-Deadline: <Date.now() + 2000>`; middleware in every service stores it in request context (AsyncLocalStorage); `resilientCall` reads it.

Rules:
- Retry only at one layer (payment-service → provider). Gateway does not retry `POST`s.
- `Retry-After` on 429/503 sets the minimum delay (handled in `retryAfterMs`).

**Verify:** provider `errorRate=0.3`: success rate near 100% with retries; provider `down`: k6 shows retries capped by budget, provider receives ≤ 1.1x requests (not 4x).

**Talking points:** timeout ambiguity, idempotency prerequisite, jitter, retry amplification, budgets, deadlines.

---

## Step 14. Circuit Breaker and Bulkhead

**Goal:** fail fast when provider is down; isolate slow dependencies.
**Topics:** #15 Circuit breaker and bulkhead.

Circuit breaker with `opossum`:
```ts
import CircuitBreaker from 'opossum';

const breaker = new CircuitBreaker(
  (dto: ChargeDto, key: string) => resilientCall((signal) => provider.charge(dto, key, signal),
    { timeoutMs: 800, retries: 2, idempotent: true, budget: providerBudget }),
  {
    timeout: 3000,                    // whole call incl. retries
    errorThresholdPercentage: 50,
    volumeThreshold: 20,              // min calls before evaluating
    rollingCountTimeout: 10000,
    resetTimeout: 10000,              // open -> half-open after 10s
  });

breaker.fallback(() => { throw new ServiceUnavailableException('PAYMENT_PROVIDER_UNAVAILABLE'); });
breaker.on('open', () => logger.warn('payment breaker OPEN'));
breaker.on('halfOpen', () => logger.info('payment breaker HALF_OPEN'));
breaker.on('close', () => logger.info('payment breaker CLOSED'));
```
When the breaker is open, the `payment.charge` handler catches `PAYMENT_PROVIDER_UNAVAILABLE`, stores the payment as `PENDING` and replies `PaymentDeferred`; the saga parks the order as `PAYMENT_DEFERRED` (Step 12) and a payment-service job retries pending charges once the breaker closes (graceful degradation, Step 16). Alternative policy: reply `PaymentFailed` and compensate immediately.

Bulkhead (semaphore, CommonJS-friendly):
```ts
export class Bulkhead {
  private active = 0;
  private queue: (() => void)[] = [];
  constructor(private maxConcurrent: number, private maxQueue: number) {}

  async run<T>(fn: () => Promise<T>): Promise<T> {
    if (this.active < this.maxConcurrent) {
      this.active++;
    } else {
      if (this.queue.length >= this.maxQueue) throw new ServiceUnavailableException('BULKHEAD_FULL');
      await new Promise<void>((resolve) => this.queue.push(resolve));   // slot is handed over on release
    }
    try { return await fn(); }
    finally {
      // hand our slot directly to the next waiter instead of decrementing: otherwise a new caller
      // could grab the free slot before the woken waiter resumes, and concurrency would exceed the limit
      const next = this.queue.shift();
      if (next) next();
      else this.active--;
    }
  }
}

const paymentBulkhead = new Bulkhead(20, 50);
await paymentBulkhead.run(() => breaker.fire(dto, key));
```
Also:
- Separate HTTP agent per dependency: `new https.Agent({ keepAlive: true, maxSockets: 50 })`.
- Separate DB pools: `order-service` uses a small dedicated pool for admin/report queries so they cannot starve checkout.

Experiments:
1. Provider `mode=down`: after ~20 calls breaker opens; responses become instant 503; provider gets ~no traffic; after 10 s half-open probes.
2. Provider `latencyMs=10000`: without bulkhead, `GET /payments/:id` (DB only) latency also degrades; with bulkhead it stays fast, excess charges get `BULKHEAD_FULL`.

**Talking points:** breaker states, thresholds, per-dependency breakers, fallback options, bulkhead in single-threaded Node.

---

## Step 15. Rate Limiting

**Goal:** distributed token bucket and sliding window in the gateway.
**Topics:** #16 Rate limiting.

Keep the Lua script from the theory doc (topic 16) as a TS string constant, not a `.lua` file: Nest's monorepo webpack build does not copy non-TS files, and `__dirname` would not point at the source folder.
```ts
// libs/common/src/ratelimit/token-bucket.lua.ts
export const TOKEN_BUCKET_LUA = `
  ...script from the theory doc: ARGV = capacity, refill_per_sec, cost; uses redis.call('TIME')...
`;

redis.defineCommand('tokenBucket', { numberOfKeys: 1, lua: TOKEN_BUCKET_LUA });
```
The script reads the clock with `redis.call('TIME')`: all gateway instances share Redis's clock, so clock skew between app hosts cannot distort refill.

Behind nginx, `req.ip` is nginx's address unless Express trusts the proxy. In `main.ts` of the gateway:
```ts
const app = await NestFactory.create<NestExpressApplication>(AppModule);
app.set('trust proxy', 1);   // take the client IP from X-Forwarded-For set by the first proxy (nginx)
```
and in nginx: `proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;`. Trust exactly the number of proxies you run; trusting more lets clients spoof their IP via the header.

Guard:
```ts
@Injectable()
export class RateLimitGuard implements CanActivate {
  constructor(@Inject('REDIS') private redis: Redis & { tokenBucket: (...a: any[]) => Promise<[number, number, number]> },
              private reflector: Reflector) {}

  async canActivate(ctx: ExecutionContext): Promise<boolean> {
    const req = ctx.switchToHttp().getRequest();
    const res = ctx.switchToHttp().getResponse();
    const cfg = this.reflector.get<RateCfg>('rate', ctx.getHandler()) ?? { capacity: 20, perSec: 10, cost: 1 };
    const id = req.user?.id ?? req.headers['x-api-key'] ?? req.ip;   // req.ip is the real client IP with trust proxy
    const key = `rl:${ctx.getClass().name}:${ctx.getHandler().name}:${id}`;
    try {
      // script returns integers: [allowed 1/0, remaining tokens (floored), retry_after_ms]
      const [allowed, remaining, retryMs] = await this.redis.tokenBucket(key, cfg.capacity, cfg.perSec, cfg.cost);
      res.setHeader('RateLimit-Limit', cfg.capacity);
      res.setHeader('RateLimit-Remaining', remaining);
      if (!allowed) {
        res.setHeader('Retry-After', Math.ceil(retryMs / 1000));
        throw new HttpException('Too Many Requests', 429);
      }
      return true;
    } catch (e) {
      if (e instanceof HttpException) throw e;
      return true;                                    // Redis down -> fail-open (document the decision)
    }
  }
}
```
Decorator: `@Rate({ capacity: 5, perSec: 1, cost: 1 })` on `POST /payments`, `@Rate({ capacity: 100, perSec: 50 })` on reads, higher `cost` on search.

Sliding window counter variant (`sliding-window.lua`) using two keys `rl:<id>:<window>` and the weighted formula from the theory doc. Compare with `@nestjs/throttler` (fixed window, in-memory/Redis storage): good enough for simple cases, but shows the boundary burst.

Experiments:
1. k6 at 50 req/s for one user: ~10 req/s pass, rest 429 with `Retry-After`.
2. Fixed window boundary: 100 requests at `:59.9` and 100 at `:00.1` pass with fixed window; not with token bucket/sliding window.
3. Run 3 gateway instances behind nginx: limit is global (Redis), not 3x.

**Talking points:** algorithms comparison, atomicity via Lua, keys (user/IP/API key), fail-open vs fail-closed, 429 semantics, rate limit vs load shedding.

---

## Step 16. Scaling and Graceful Degradation

**Goal:** horizontal scaling behind nginx; controlled degradation instead of total failure.
**Topics:** #17 Scaling and graceful degradation.

### 16.1 Horizontal scaling
- Make services stateless (no in-memory sessions; caches are OK because they are rebuildable).
- Containerize the apps. One multi-stage `Dockerfile` for the monorepo, parameterized by app name:
  ```dockerfile
  FROM node:20-alpine AS build
  WORKDIR /src
  COPY package*.json ./
  RUN npm ci
  COPY .. .
  ARG APP
  RUN npx nest build ${APP}

  FROM node:20-alpine
  WORKDIR /app
  ARG APP
  ENV NODE_ENV=production
  COPY package*.json ./
  RUN npm ci --omit=dev
  COPY --from=build /src/dist/apps/${APP} ./dist
  CMD ["node", "dist/main.js"]
  ```
  Add the apps to `infra/docker-compose.yml` **without host port mappings** (a fixed `ports: ["3001:3001"]` makes `--scale` fail because replicas would fight for the same host port); only nginx publishes a port:
  ```yaml
    api-gateway:
      build: { context: .., dockerfile: Dockerfile, args: { APP: api-gateway } }
      environment: { REDIS_URL: redis://redis:6379, ORDER_SERVICE_URL: http://order-service:3001 }
      expose: ["3000"]
    order-service:
      build: { context: .., dockerfile: Dockerfile, args: { APP: order-service } }
      environment:
        DB_URL: postgres://app:app@pg-primary:5432/orders
        DB_REPLICA_URL: postgres://app:app@pg-replica:5432/orders
        REDIS_URL: redis://redis:6379
        AMQP_URL: amqp://app:app@rabbitmq:5672
      expose: ["3001"]
    # payment-service, inventory-service, notification-service, fake-payment-provider: same pattern
    nginx:
      image: nginx:1.27-alpine
      volumes: ["./nginx/nginx.conf:/etc/nginx/conf.d/default.conf:ro"]
      ports: ["80:80"]
      depends_on: [api-gateway]
  ```
- `docker compose up -d --scale api-gateway=3 --scale order-service=3`. nginx balances clients across gateways; gateways reach order-service replicas through Docker DNS (`order-service` resolves to all replica IPs).
  `infra/nginx/nginx.conf`:
  ```nginx
  upstream api_gateway { least_conn; server api-gateway:3000 max_fails=3 fail_timeout=10s; }
  server {
    listen 80;
    location / {
      proxy_pass http://api_gateway;
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;   # real client IP for rate limiting
      proxy_read_timeout 3s;
      proxy_next_upstream error timeout http_503;                      # POST is not retried unless non_idempotent is set
    }
  }
  ```
  > Open-source nginx resolves `api-gateway` once at startup. After changing the replica count, run `docker compose restart nginx` (or use a `resolver 127.0.0.11` + variable in `proxy_pass` for dynamic resolution).
- Watch DB connections: 3 instances × pool 10 = 30. Add **PgBouncer** in transaction pooling mode when scaling further. Transaction mode breaks session state: session `SET`, session advisory locks, `LISTEN/NOTIFY`, temp tables, and prepared statements (the `pg` driver's named statements). PgBouncer ≥ 1.21 can track protocol-level prepared statements (`max_prepared_statements`); otherwise avoid named prepared statements.
- Scale consumers by queue depth (manually here; KEDA in Kubernetes).

### 16.2 Load shedding on event loop lag
```ts
import { monitorEventLoopDelay } from 'perf_hooks';
const h = monitorEventLoopDelay({ resolution: 20 }); h.enable();

@Injectable()
export class LoadShedMiddleware implements NestMiddleware {
  private inflight = 0;
  use(req: any, res: any, next: () => void) {
    const lagMs = h.mean / 1e6;
    const lowPriority = req.headers['x-priority'] === 'low' || req.path.startsWith('/recommendations');
    if ((lagMs > 100 && lowPriority) || lagMs > 300 || this.inflight > 500) {
      res.setHeader('Retry-After', '1');
      return res.status(503).json({ error: 'OVERLOADED' });
    }
    this.inflight++;
    // 'close' fires both after a normal response and when the client disconnects early;
    // 'finish' would never fire for aborted requests, so the counter would only grow
    res.once('close', () => this.inflight--);
    next();
  }
}
setInterval(() => h.reset(), 1000);
```

### 16.3 Feature flags / kill switches
Simple Redis-backed flags (`flags:recommendations=off`) read with 5 s in-process cache. Endpoint `GET /orders/:id` includes `recommendations` only if the flag is on and the recommendations call (with breaker) succeeds; otherwise returns `recommendations: []`.

### 16.4 Degraded modes
| Failure | Degraded behavior |
|---|---|
| Payment provider down (breaker open) | Accept order as `PAYMENT_DEFERRED`, retry via saga timeout job, notify user |
| Redis down | Cache fail-open (DB reads), rate limiter fail-open with local in-memory limiter |
| Replica down | Reads go to primary (TypeORM fallback or health-checked routing) |
| Primary down | Read-only mode: `GET` from replica works, `POST` returns 503 with clear message |
| RabbitMQ down | Outbox accumulates rows; relay resumes when broker returns; no data loss |
| DB slow | Serve stale cache (`stale-if-error`), shed low-priority endpoints |

### 16.5 Health checks and graceful shutdown
- `@nestjs/terminus`: `/health/live` (process only), `/health/ready` (DB primary ping with short timeout, RabbitMQ connection).
- `app.enableShutdownHooks()`; on `SIGTERM`: mark not ready, stop consumers (`channel.cancel`), wait for in-flight requests, stop relay, close pools.

**Verify:** scenarios in Step 17 produce the degraded behaviors in the table, not 100% errors.

**Talking points:** stateless services, DB as real bottleneck, load shedding vs rate limiting, backpressure, liveness must not check deps, critical path prioritization.

---

## Step 17. Chaos and Load Testing

**Goal:** prove resilience under load and failures.
**Topics:** all of Week 03, plus replication/caching.

`load/create-orders.js` (k6):
```js
import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    steady: { executor: 'constant-arrival-rate', rate: 100, timeUnit: '1s', duration: '3m', preAllocatedVUs: 200 },
  },
  thresholds: { http_req_failed: ['rate<0.05'], http_req_duration: ['p(99)<1500'] },
};

export default function () {
  const res = http.post('http://localhost/orders',
    JSON.stringify({ customerId: uuidv4(), items: [{ sku: 'A1', qty: 1 }] }),
    { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': uuidv4() } });
  check(res, { accepted: (r) => r.status === 202 || r.status === 429 });
}
```

Chaos scenarios (run during load; `scripts/chaos.sh` wraps `docker compose stop/pause` and provider `/chaos` calls):

| # | Action | Expected |
|---|---|---|
| 1a | `SELECT pg_wal_replay_pause();` on replica for 30 s | Replica reads are stale; read-your-writes still correct (author reads go to primary) |
| 1b | `docker compose stop pg-replica` | Replica reads fail fast (connect timeout) and fall back to primary; no request hangs |
| 2 | Provider `errorRate=0.3` | Retries absorb errors; success ~100%; provider load ≤ 1.1x |
| 3 | Provider `mode=down` | Breaker opens in seconds; orders `PAYMENT_DEFERRED`/compensated; no thread/socket pile-up |
| 4 | Provider `latencyMs=10000` | Bulkhead rejects excess; other endpoints p99 unaffected |
| 5 | `docker compose stop rabbitmq` 60 s | API still accepts orders; outbox grows; drains after restart; no lost events |
| 6 | Kill a consumer mid-message | Redelivery; inbox prevents duplicate effects |
| 7 | `docker compose stop redis` | Cache/limiter fail-open; DB load increases; system stays up |
| 8 | Flood one user at 500 req/s | 429s for that user only; others unaffected |
| 9 | Flush Redis while hot key under load | Lock + single-flight: DB sees ~1 query per key, not 500 |
| 10 | Stop one `order-service` replica | nginx routes to remaining; graceful shutdown finishes in-flight |

Record results (screenshots of k6 summary, RabbitMQ UI, breaker logs) in `docs/chaos-results.md`. This is excellent interview material: "I measured it".

---

## Step 18. Observability Minimum

**Goal:** be able to explain what happened during chaos tests.

- Structured JSON logs (pino) with `correlationId`, `orderId`, `service` on every line; correlation ID propagated in HTTP headers and message headers.
- Metrics (optional, `prom-client` + Prometheus + Grafana in compose):
  - HTTP: RPS, error rate, p50/p95/p99 per route
  - Breaker state, bulkhead queue length, retry count, retry budget tokens
  - Cache hit/miss, rate-limit rejections
  - Outbox lag (`now() - min(created_at) WHERE published_at IS NULL`), queue depth, DLQ size
  - Consumer lag for streams (Step 10.1): latest stream offset − offset stored by the reader (Kafka: log-end offset − committed offset per partition). Growing lag = consumers slower than producers.
  - Replication lag, DB pool usage
- Alerts worth naming in interviews: outbox lag > 1 min, DLQ > 0, breaker open > 5 min, replication lag > 10 s.

---

## Appendix A. Topic → Step Map

| # | Curriculum topic | Theory section | Practice step(s) |
|---|---|---|---|
| 1 | Data models and DB choice | 1 | 3 |
| 2 | Indexes, B-Tree vs LSM, slow queries | 2 | 4 |
| 3 | Transactions and ACID | 3 | 5 |
| 4 | Isolation levels and anomalies | 4 | 5 |
| 5 | Replication | 5 | 2, 6 |
| 6 | Consensus and Raft | 6 | 8 |
| 7 | Sharding and partitioning | 7 | 7 |
| 8 | CAP and PACELC | 8 | 8 |
| 9 | Caching strategies | 9 | 9 |
| 10 | Cache problems | 10 | 9, 17 |
| 11 | Queues and brokers (MQ vs stream, Kafka) | 11 | 10, 10.1 |
| 12 | Idempotency and transactional outbox | 12 | 11 |
| 13 | Distributed transactions (2PC, 3PC, saga) | 13 | 12 |
| 14 | Retries, timeouts, backoff | 14 | 13 |
| 15 | Circuit breaker and bulkhead | 15 | 14 |
| 16 | Rate limiting | 16 | 15 |
| 17 | Scaling and graceful degradation | 17 | 16, 17 |

## Appendix B. Final Demo Checklist

- [ ] `EXPLAIN ANALYZE` before/after index, keyset pagination
- [ ] Race script: naive vs 4 fixes, results table
- [ ] Write skew reproduced under REPEATABLE READ, fixed under SERIALIZABLE
- [ ] Replica lag shown; read-your-writes works; sync vs async latency measured
- [ ] Partition pruning shown; consistent hashing moved-keys experiment
- [ ] etcd leader killed, re-election observed, minority unavailable
- [ ] CAP/PACELC table for components
- [ ] Cache: hit ratio, stampede protection, negative cache, Redis-down behavior
- [ ] RabbitMQ: redelivery, retry queue, DLQ, delivery limit, publisher confirms
- [ ] Stream replay from `first` offset; two independent readers
- [ ] Double `POST /payments` with same key → one charge
- [ ] Broker down → outbox drains later, nothing lost
- [ ] Saga happy path + compensation + crash recovery
- [ ] Retries with jitter + budget; no retry storm when provider down
- [ ] Breaker opens/half-opens/closes; bulkhead isolates slow provider
- [ ] Token bucket 429 + Retry-After; global across 3 gateway instances
- [ ] Load shedding + feature flag + degraded modes table verified
- [ ] Chaos results documented
