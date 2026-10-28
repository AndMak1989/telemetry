import { describe, expect, it } from 'vitest';
import type { OrderConfirmedDetail } from '@myapp/shared';
import { handler } from '../src/functions/notifications-worker';
import { eventBridgeEnvelope, fakeContext, sqsEvent, useTestDb } from './helpers';

const getDb = useTestDb();

const notifications = async () => (await getDb()).collection('notifications').find().toArray();
const confirmed = (orderId: string, eventId = 'evt-1') =>
  eventBridgeEnvelope('OrderConfirmed', { version: 1, orderId, confirmedAt: new Date().toISOString() } satisfies OrderConfirmedDetail, eventId);

describe('notifications-worker', () => {
  it('records one notification per confirmed order', async () => {
    const out = await handler(sqsEvent(confirmed('o-1'), confirmed('o-2', 'evt-2')), fakeContext('notifications-worker'));

    expect(out.batchItemFailures).toEqual([]);
    expect((await notifications()).map((n) => n.orderId).sort()).toEqual(['o-1', 'o-2']);
    expect(await notifications()).toEqual(
      expect.arrayContaining([expect.objectContaining({ orderId: 'o-1', kind: 'order-confirmed', createdAt: expect.any(Date) })]),
    );
  });

  it('dedupes on the order, so a republished event (new EventBridge id) sends nothing twice', async () => {
    await handler(sqsEvent(confirmed('o-1', 'evt-1')), fakeContext('notifications-worker'));
    const out = await handler(sqsEvent(confirmed('o-1', 'evt-replayed')), fakeContext('notifications-worker'));

    expect(out.batchItemFailures).toEqual([]);
    expect(await notifications()).toHaveLength(1);
  });

  it('ignores other event types without failing them', async () => {
    const out = await handler(
      sqsEvent(eventBridgeEnvelope('OrderShipped', { orderId: 'o-1' })),
      fakeContext('notifications-worker'),
    );

    expect(out.batchItemFailures).toEqual([]); // a retry would never succeed, so don't send it to the DLQ
    expect(await notifications()).toHaveLength(0);
  });

  it('fails only malformed records', async () => {
    const out = await handler(
      sqsEvent(
        confirmed('o-1'),
        'not json',
        { 'detail-type': 'OrderConfirmed' },                                // not an EventBridge envelope
        eventBridgeEnvelope('OrderConfirmed', { version: 2, orderId: 'o-3' }), // unknown detail version
      ),
      fakeContext('notifications-worker'),
    );

    expect(out.batchItemFailures).toEqual([{ itemIdentifier: 'm-1' }, { itemIdentifier: 'm-2' }, { itemIdentifier: 'm-3' }]);
    expect((await notifications()).map((n) => n.orderId)).toEqual(['o-1']);
  });
});
