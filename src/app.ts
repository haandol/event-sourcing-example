import express from 'express';
import { Kafka, CompressionTypes } from 'kafkajs';
import { Command } from './interfaces/event';

const kafka = new Kafka({
  clientId: 'app',
  brokers: ['localhost:9092'],
});

const topic = 'account1';
const producer = kafka.producer();

const createMessage = (event: Command.CommandEvent) => ({
  key: event.id,
  value: event.toJSON(),
});

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
const port = 3000;

app.post('/accounts', async (req, res) => {
  const { accountId } = req.body;
  const event = new Command.CreateAccount({ accountId });
  producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [createMessage(event)],
    })
    .then(console.log)
    .catch(e => console.error(`[app] ${e.message}`, e));
  res.json(event.toJSON());
});

app.post('/accounts/:id/deposit', async (req, res) => {
  const accountId = req.params.id;
  const { amount } = req.body;
  const event = new Command.DepositMoney(
    { accountId, amount: parseInt(amount) },
  );
  producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [createMessage(event)],
    })
    .then(console.log)
    .catch(e => console.error(`[app] ${e.message}`, e));
  res.json(event.toJSON());
});

app.post('/accounts/:id/withdraw', async (req, res) => {
  const accountId = req.params.id;
  const { amount } = req.body;
  const event = new Command.WithdrawMoney(
    { accountId, amount: parseInt(amount) },
  );
  producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [createMessage(event)],
    })
    .then(console.log)
    .catch(e => console.error(`[app] ${e.message}`, e));
  res.json(event.toJSON());
});

app.listen(port, async () => {
  await producer.connect();
  console.log('Server started...');
});

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
      await producer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});