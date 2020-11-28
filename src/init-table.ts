import AWS from 'aws-sdk';

const ddb = new AWS.DynamoDB({
  endpoint: 'http://localhost:8000',
  region: 'ap-northeast-2',
});

const createCommandTable = async (tableName: string) => {
  await ddb.createTable({
    TableName: tableName,
    AttributeDefinitions: [
      { AttributeName: 'id', AttributeType: 'S' },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 5,
      WriteCapacityUnits: 5,
    },
    KeySchema: [
      { AttributeName: 'id', KeyType: 'HASH' },
    ],
  }).promise();
}

const createDomainTable = async (tableName: string) => {
  await ddb.createTable({
    TableName: tableName,
    AttributeDefinitions: [
      { AttributeName: 'aggregateId', AttributeType: 'S' },
      { AttributeName: 'serialNumber', AttributeType: 'S' },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 5,
      WriteCapacityUnits: 5,
    },
    KeySchema: [
      { AttributeName: 'aggregateId', KeyType: 'HASH' },
      { AttributeName: 'serialNumber', KeyType: 'RANGE' },
    ],
  }).promise();
}

const createReadTable = async (tableName: string) => {
  await ddb.createTable({
    TableName: tableName,
    AttributeDefinitions: [
      { AttributeName: 'accountId', AttributeType: 'S' },
      { AttributeName: 'amount', AttributeType: 'S' },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 5,
      WriteCapacityUnits: 5,
    },
    KeySchema: [
      { AttributeName: 'accountId', KeyType: 'HASH' },
    ],
  }).promise();
}

const scan = async (tableName: string): Promise<any> => {
  const resp = await ddb.scan({
    TableName: tableName,
  }).promise();
  return JSON.stringify(resp.Items);
}

const run = async () => {
  const commandTableName = 'command';
  await createCommandTable(commandTableName);
  console.log('domain scan: ', await scan(commandTableName));

  const domainTableName = 'domain';
  await createDomainTable(domainTableName);
  console.log('command scan: ', await scan(domainTableName));

  const readTableName = 'read';
  await createReadTable(readTableName);
  console.log('read scan: ', await scan(readTableName));
}

run().catch(console.error);