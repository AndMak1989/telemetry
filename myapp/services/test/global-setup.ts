import type { TestProject } from 'vitest/node';
import { MongoMemoryReplSet } from 'mongodb-memory-server';

export default async function setup(project: TestProject) {
  const rs = await MongoMemoryReplSet.create({ replSet: { count: 1 } });
  project.provide('mongoUri', rs.getUri());
  return async () => { await rs.stop(); };
}

declare module 'vitest' {
  export interface ProvidedContext { mongoUri: string }
}
