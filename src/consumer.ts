import AWS from 'aws-sdk';
import { Kafka, CompressionTypes } from 'kafkajs';
import { Command, Domain, Account, getLaestSerialNumber } from './interfaces/event';

const kafka = new Kafka({
  clientId: 'consumer',
  brokers: ['localhost:9092'],
});

const ddb = new AWS.DynamoDB({
  endpoint: 'http://localhost:8000',
  region: 'ap-northeast-2',
});

const topic = 'account1';
const consumer = kafka.consumer({ groupId: 'test-group' })
const producer = kafka.producer();

const rehydrate = (items: AWS.DynamoDB.ItemList) => {
  let currentState = undefined;
  for (const item of items) {
    const event = Domain.DomainEvent.fromDict({
      aggregateId: item.aggregateId.S!,
      serialNumber: item.serialNumber.S!,
      type: item.eventType.S!,
      data: JSON.parse(item.data.S!),
    });
    console.log('[Rehydrate]: ', event);
    if (!currentState) {
      currentState = new Account(event);
      continue;
    }

    if (event.type === 'MoneyDeposited') {
      currentState.deposit(event);
    } else if (event.type === 'MoneyWithdrawn') {
      currentState.withdrawn(event);
    } else {
      console.error(event.toJSON());
    }
  }
  return currentState;
};

const createMessage = (event: Domain.DomainEvent) => ({
  key: event.aggregateId,
  value: event.toJSON(),
});
const publish = async (event: Domain.DomainEvent) => {
  await producer.send({
    topic,
    compression: CompressionTypes.GZIP,
    messages: [createMessage(event)],
  });
  console.log(`[${event.type}] event published`);
}

const eventHandlers: {[key: string]: any} = {
  'CreateAccount': async (dict: any) => {
    const event = Command.CreateAccount.fromDict(dict);
    await event.store();

    const { accountId } = event.data;
    const resp = await ddb.query({
      TableName: 'domain',
      KeyConditionExpression: `aggregateId = :accountId`,
      ExpressionAttributeValues: {
        ':accountId': { S: accountId },
      },
    }).promise();
    if (resp.Items!.length > 0) throw Error('Account already exists');

    const serialNumber = await getLaestSerialNumber(event.data.accountId);
    await publish(new Domain.AccountCreated({
      serialNumber: serialNumber + 1,
      aggregateId: event.data.accountId,
      data: event.data,
    }));
    console.log(event);
  },
  'DepositMoney': async (dict: any) => {
    const event = Command.DepositMoney.fromDict(dict);
    await event.store();

    const serialNumber = await getLaestSerialNumber(event.data.accountId);
    await publish(new Domain.MoneyDeposited({
      serialNumber: serialNumber + 1,
      aggregateId: event.data.accountId,
      data: event.data,
    }));
    console.log(event);
  },
  'WithdrawMoney': async (dict: any) => {
    const event = Command.WithdrawMoney.fromDict(dict);
    await event.store();

    const { accountId, amount } = event.data;
    const resp = await ddb.query({
      TableName: 'domain',
      KeyConditionExpression: `aggregateId = :accountId`,
      ExpressionAttributeValues: {
        ':accountId': { S: accountId },
      },
      Limit: 100,
    }).promise();
    console.log('[Withdraw]: ', resp.Items!)
    const currentState = rehydrate(resp.Items!);
    if (currentState!.amount < amount) {
      throw Error(`current amount is short: ${currentState!.amount} < ${amount}`);
    }

    console.log('[Rehydrated]: ', currentState);

    const serialNumber = await getLaestSerialNumber(event.data.accountId);
    await publish(new Domain.MoneyWithdrawn({
      serialNumber: serialNumber + 1,
      aggregateId: event.data.accountId,
      data: event.data,
    }));
    console.log(event);
  },
  'AccountCreated': async (dict: any) => {
    const event = Domain.AccountCreated.fromDict(dict);
    await event.store();
    console.log(event);
  },
  'MoneyDeposited': async (dict: any) => {
    const event = Domain.MoneyDeposited.fromDict(dict);
    await event.store();
    console.log(event);
  },
  'MoneyWithdrawn': async (dict: any) => {
    const event = Domain.MoneyWithdrawn.fromDict(dict);
    await event.store();
    console.log(event);
  },
}

const run = async () => {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`- ${prefix} ${message.key}#${message.value}`);
      const dict: any = JSON.parse(message.value!.toString());
      const handler = eventHandlers[dict.type];
      try {
        await handler(dict);
      } catch (e) {
        console.error(e);
      }
    },
  });
}

run().catch(e => console.error(`[consumer] ${e.message}`, e));

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.map(type => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`);
      await consumer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.map(type => {
  process.once(<NodeJS.Signals>type, async () => {
    try {
      await consumer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});