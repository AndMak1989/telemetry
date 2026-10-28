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
  traceparent: z.string().optional(), // W3C trace context, so the consumer can join the producer's trace
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
