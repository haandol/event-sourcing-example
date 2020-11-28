import AWS from 'aws-sdk';

const ddb = new AWS.DynamoDB({
  endpoint: 'http://localhost:8000',
  region: 'ap-northeast-2',
});

const getSerialNumber = async (aggregateId: string): Promise<number> => {
  const items = await ddb.query({
    TableName: 'domain',
    AttributesToGet: ['id'],
    KeyConditions: {
      'id': {
        AttributeValueList: [
          { S: aggregateId }
        ],
        ComparisonOperator: 'EQ',
      }
    },
    ScanIndexForward: false,
    Limit: 1,
  }).promise();
  console.log(items);
  return 1;
};

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

const scan = async (tableName: string): Promise<any> => {
  const resp = await ddb.scan({
    TableName: tableName,
  }).promise();
  return JSON.stringify(resp.Items);
}

const run = async (id: string) => {
  // const commandTableName = 'command';
  // await createCommandTable(commandTableName);
  // const domainTableName = 'domain';
  // await createDomainTable(domainTableName);
  // const serialNumber = await getSerialNumber(id);
  // console.log(serialNumber);
  console.log(await scan('domain'));
  // console.log(await scan('command'));
}

run('1').catch(console.error);