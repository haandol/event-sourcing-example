import AWS from 'aws-sdk';
import { v4 as uuidv4 } from 'uuid';

const ddb = new AWS.DynamoDB({
  endpoint: 'http://localhost:8000',
  region: 'ap-northeast-2',
});

export const getLaestSerialNumber = async (aggregateId: string): Promise<number> => {
  const items = await ddb.query({
    TableName: 'domain',
    AttributesToGet: ['aggregateId', 'serialNumber'],
    KeyConditions: {
      'aggregateId': {
        AttributeValueList: [
          { S: `${aggregateId}`}
        ],
        ComparisonOperator: 'EQ',
      }
    },
    ScanIndexForward: false,
    Limit: 1,
  }).promise();
  return (items.Items!.length === 1) ? parseInt(items.Items![0].serialNumber.S!) : 0;
};

export namespace Command {
  export class CommandEvent {
    public id: string;
    public timestamp: number;
    public type: string;
    public data: any;

    constructor(data: any) {
      this.id = uuidv4().toString();
      this.timestamp = +new Date();
      this.type = this.constructor.name;
      this.data = data;
    }

    async store() {
      await ddb.putItem({
        TableName: 'command',
        Item: {
          id: { S: this.id },
          timestamp: { N: `${this.timestamp}` },
          eventType: { S: this.type },
          data: { S: JSON.stringify(this.data) },
        },
        ConditionExpression: 'attribute_not_exists(id)',
      }).promise();
    }

    toJSON() {
      return JSON.stringify({
        id: this.id,
        timestamp: this.timestamp,
        type: this.type,
        data: this.data,
      });
    }
    
    static fromDict(dict: any) {
      if ('CreateAccount' === dict.type) {
        return new CreateAccount(dict.data);
      } else if ('DepositMoney' === dict.type) {
        return new DepositMoney(dict.data);
      } else if ('WithdrawMoney' === dict.type) {
        return new WithdrawMoney(dict.data);
      } else {
        return new CommandEvent(dict.data);
      }
    }
  }

  export class CreateAccount extends CommandEvent {
  }

  export class DepositMoney extends CommandEvent {
  }

  export class WithdrawMoney extends CommandEvent {
  }
}

export namespace Domain {
  interface DomainParams {
    serialNumber: number;
    aggregateId: string;
    data: any;
  }

  export class DomainEvent {
    public serialNumber: number;
    public timestamp: number;
    public aggregateId: string;
    public type: string;
    public data: any;

    constructor(params: DomainParams) {
      this.serialNumber = params.serialNumber;
      this.timestamp = +new Date();
      this.aggregateId = params.aggregateId;
      this.type = this.constructor.name;
      this.data = params.data;
    }

    async store() {
      await ddb.putItem({
        TableName: 'domain',
        Item: {
          aggregateId: { S: `${this.aggregateId}` },
          serialNumber: { S: `${this.serialNumber}` },
          timestamp: { N: `${this.timestamp}` },
          eventType: { S: this.type },
          data: { S: JSON.stringify(this.data) },
        },
        ConditionExpression: 'attribute_not_exists(aggregateId) AND attribute_not_exists(serialNumber)',
      }).promise();
    }

    toJSON() {
      return JSON.stringify({
        serialNumber: this.serialNumber,
        timestamp: this.timestamp,
        aggregateId: this.aggregateId,
        type: this.type,
        data: this.data,
      });
    }

    static fromDict(dict: any) {
      if ('AccountCreated' === dict.type) {
        return new AccountCreated(dict);
      } else if ('MoneyDeposited' === dict.type) {
        return new MoneyDeposited(dict);
      } else if ('MoneyWithdrawn' === dict.type) {
        return new MoneyWithdrawn(dict);
      } else {
        return new DomainEvent(dict);
      }
    }
  }

  export class AccountCreated extends DomainEvent {
  }

  export class MoneyDeposited extends DomainEvent {
  }

  export class MoneyWithdrawn extends DomainEvent {
  }
}

export class Account {
  public id: string;
  public amount: number;

  constructor(event: Domain.AccountCreated) {
    this.id = event.aggregateId;
    this.amount = 0;
  }

  deposit (event: Domain.MoneyDeposited) {
    this.amount += parseInt(event.data.amount);
  }

  withdrawn (event: Domain.MoneyWithdrawn) {
    const amount = parseInt(event.data.amount);
    if (this.amount < amount) throw Error('Stored event is invalid');
    this.amount -= amount;
  }
}