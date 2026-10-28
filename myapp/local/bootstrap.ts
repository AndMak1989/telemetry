import { writeFileSync } from 'node:fs';
import { CreateQueueCommand, GetQueueAttributesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { CreateEventBusCommand, EventBridgeClient, PutRuleCommand, PutTargetsCommand } from '@aws-sdk/client-eventbridge';

const sqs = new SQSClient({});
const eb = new EventBridgeClient({});
const BUS = 'myapp-local-bus';

async function arnOf(url: string) {
  const r = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ['QueueArn'] }));
  return r.Attributes!.QueueArn!;
}

async function queueWithDlq(name: string) {
  const dlq = await sqs.send(new CreateQueueCommand({
    QueueName: `${name}-dlq`,
    Attributes: { MessageRetentionPeriod: String(14 * 24 * 3600) },
  }));
  const q = await sqs.send(new CreateQueueCommand({
    QueueName: name,
    Attributes: {
      VisibilityTimeout: '30',          // shorter than prod so local retries are quick
      ReceiveMessageWaitTimeSeconds: '20',
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: await arnOf(dlq.QueueUrl!), maxReceiveCount: 3 }),
    },
  }));
  return { url: q.QueueUrl!, arn: await arnOf(q.QueueUrl!) };
}

const orders = await queueWithDlq('orders');
const notifications = await queueWithDlq('notifications');

try {
  await eb.send(new CreateEventBusCommand({ Name: BUS }));
} catch (err) {
  if ((err as Error).name !== 'ResourceAlreadyExistsException') throw err;
}
await eb.send(new PutRuleCommand({
  Name: 'order-confirmed-to-notifications',
  EventBusName: BUS,
  EventPattern: JSON.stringify({ source: ['myapp.orders'], 'detail-type': ['OrderConfirmed'] }),
}));
await eb.send(new PutTargetsCommand({
  Rule: 'order-confirmed-to-notifications',
  EventBusName: BUS,
  Targets: [{ Id: 'notifications-queue', Arn: notifications.arn }],
}));

writeFileSync('.env.generated', [
  `ORDERS_QUEUE_URL=${orders.url}`,
  `NOTIFICATIONS_QUEUE_URL=${notifications.url}`,
  `EVENT_BUS_NAME=${BUS}`,
].join('\n') + '\n');
console.log('Local AWS resources ready → .env.generated');
