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
