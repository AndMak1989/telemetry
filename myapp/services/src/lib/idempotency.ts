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
