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
