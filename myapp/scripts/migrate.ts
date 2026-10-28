import { MongoClient } from 'mongodb';
import { ensureIndexes } from '../services/src/lib/schema';

const client = new MongoClient(process.env.MONGO_URI!);
await client.connect();
await ensureIndexes(client.db(process.env.MONGO_DB_NAME));
console.log('Indexes ready');
await client.close();
