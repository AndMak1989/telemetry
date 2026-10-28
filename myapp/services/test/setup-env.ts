import { inject } from 'vitest';
process.env.MONGO_URI = inject('mongoUri');
process.env.MONGO_DB_NAME = `test_${process.env.VITEST_POOL_ID ?? '0'}`;
process.env.ORDERS_QUEUE_URL = 'https://sqs.local/000000000000/orders';
process.env.EVENT_BUS_NAME = 'test-bus';
