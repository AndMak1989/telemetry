import { afterAll, beforeAll, beforeEach } from 'vitest';
import { mockClient } from 'aws-sdk-client-mock';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
import type { APIGatewayProxyEventV2WithJWTAuthorizer, SQSEvent, SQSRecord } from 'aws-lambda';
import type { Db } from 'mongodb';
import { closeMongo, getDb } from '../src/lib/mongo';
import { ensureIndexes } from '../src/lib/schema';

export { fakeContext } from '../../local/fake-context';

/**
 * Real Mongo (in-memory replica set, one database per Vitest worker), emptied before every test.
 * Indexes match a migrated environment, so transactions never have to create collections.
 */
export function useTestDb(): () => Promise<Db> {
  beforeAll(async () => ensureIndexes(await getDb()));
  beforeEach(async () => {
    const db = await getDb();
    await Promise.all(
      ['orders', 'outbox', 'processed_messages', 'notifications'].map((c) => db.collection(c).deleteMany({})),
    );
  });
  afterAll(closeMongo);
  return getDb;
}

/** SQS and EventBridge mocked at the SDK level; by default every send succeeds. */
export function useAwsMocks() {
  const sqs = mockClient(SQSClient);
  const eventBridge = mockClient(EventBridgeClient);
  beforeEach(() => {
    sqs.reset();
    eventBridge.reset();
    sqs.on(SendMessageCommand).resolves({ MessageId: 'sqs-1' });
    eventBridge.on(PutEventsCommand).resolves({ FailedEntryCount: 0, Entries: [{ EventId: 'eb-1' }] });
  });
  return {
    sqs,
    eventBridge,
    sentMessages: () => sqs.commandCalls(SendMessageCommand).map((c) => c.args[0].input),
    putEvents: () => eventBridge.commandCalls(PutEventsCommand).flatMap((c) => c.args[0].input.Entries ?? []),
  };
}

export const ALL_PERMISSIONS = ['read:orders', 'write:orders'];

interface ApiEventOptions {
  body?: unknown;
  rawBody?: string;           // sent as-is, e.g. invalid JSON
  base64?: boolean;
  /** Default: the flattened string form the HTTP API JWT authorizer produces, "[read:orders write:orders]". */
  permissions?: string[] | string;
  sub?: string | null;        // null = no sub claim
  query?: Record<string, string>;
  pathParameters?: Record<string, string>;
}

/** An HTTP API (v2) event as API Gateway delivers it after the JWT authorizer. */
export function apiEvent(routeKey: string, o: ApiEventOptions = {}): APIGatewayProxyEventV2WithJWTAuthorizer {
  const text = o.rawBody ?? (o.body === undefined ? undefined : JSON.stringify(o.body));
  const permissions = o.permissions ?? `[${ALL_PERMISSIONS.join(' ')}]`;
  const claims: Record<string, unknown> = { permissions };
  if (o.sub !== null) claims.sub = o.sub ?? 'user-1';
  return {
    routeKey,
    body: text !== undefined && o.base64 ? Buffer.from(text).toString('base64') : text,
    isBase64Encoded: Boolean(o.base64),
    headers: {},
    queryStringParameters: o.query,
    pathParameters: o.pathParameters,
    requestContext: { requestId: 'req-1', authorizer: { jwt: { claims, scopes: [] } } },
  } as unknown as APIGatewayProxyEventV2WithJWTAuthorizer;
}

/** An SQS batch. Each body is JSON-encoded unless it is already a string. Message ids are m-0, m-1, … */
export function sqsEvent(
  ...messages: Array<unknown | { body: unknown; attributes: Record<string, string> }>
): SQSEvent {
  return {
    Records: messages.map((m, i) => {
      const withAttrs = typeof m === 'object' && m !== null && 'attributes' in m && 'body' in m;
      const body = withAttrs ? (m as { body: unknown }).body : m;
      const attributes = withAttrs ? (m as { attributes: Record<string, string> }).attributes : {};
      return {
        messageId: `m-${i}`,
        body: typeof body === 'string' ? body : JSON.stringify(body),
        messageAttributes: Object.fromEntries(
          Object.entries(attributes).map(([k, v]) => [k, { dataType: 'String', stringValue: v }]),
        ),
      } as unknown as SQSRecord;
    }),
  };
}

/** The envelope EventBridge puts in the SQS body when a rule targets a queue. */
export function eventBridgeEnvelope(detailType: string, detail: unknown, id = 'evt-1') {
  return { id, 'detail-type': detailType, source: 'myapp.orders', time: new Date().toISOString(), detail };
}
