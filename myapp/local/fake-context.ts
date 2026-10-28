import { randomUUID } from 'node:crypto';
import type { Context } from 'aws-lambda';

export function fakeContext(functionName: string, timeoutMs = 30_000): Context {
  const deadline = Date.now() + timeoutMs;
  return {
    callbackWaitsForEmptyEventLoop: false,
    functionName,
    functionVersion: '$LATEST',
    invokedFunctionArn: `arn:aws:lambda:us-east-1:000000000000:function:${functionName}`,
    memoryLimitInMB: '512',
    awsRequestId: randomUUID(),
    logGroupName: `/aws/lambda/${functionName}`,
    logStreamName: 'local',
    getRemainingTimeInMillis: () => Math.max(0, deadline - Date.now()),
    done: () => {},
    fail: () => {},
    succeed: () => {},
  };
}
