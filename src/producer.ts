import { Kafka, CompressionTypes } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'producer',
  brokers: ['localhost:9092'],
});

const topic = 'topic-test';
const producer = kafka.producer();

const getRandomNumber = () => Math.round(Math.random() * 10);
const createMessage = (num: number) => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}`,
});

const sendMessage = () => {
  const n = getRandomNumber();
  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [createMessage(n)],
    })
    .then(console.log)
    .catch(e => console.error(`[producer] ${e.message}`, e));
}

const run = async () => {
  await producer.connect();
  setInterval(sendMessage, 3000);
}

run().catch(e => console.error(`[producer] ${e.message}`, e));

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.map(type => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`);
      await producer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.map(type => {
  process.once(<NodeJS.Signals>type, async () => {
    try {
      console.log(`process.once ${type}`);
      await producer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});