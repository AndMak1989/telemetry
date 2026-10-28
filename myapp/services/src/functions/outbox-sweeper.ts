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
