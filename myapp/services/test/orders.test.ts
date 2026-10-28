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
