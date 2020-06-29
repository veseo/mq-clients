import assert from 'assert';
import amqplib from 'amqplib';
import PQueue from 'p-queue';

import { MQClient, DataPayload, CallbackFunc } from '../MQClient';
import { MQConnectionError } from '../errors';
import makeEnum from '../utils/makeEnum';

const ExchangeType = makeEnum({
  Fanout: 'fanout',
  Direct: 'direct',
});

type ExchangeType = (typeof ExchangeType)[keyof typeof ExchangeType];

interface ExchangeConfig {
  type: ExchangeType;
  name?: string;
  durable?: boolean;
}

interface RabbitMQConstructorParams {
  amqp?: amqplib.Options.Connect;
  exchange: ExchangeConfig;
  retryTimeout?: number;
  debug?: boolean;
}

class RabbitMQClient implements MQClient {
  private readonly amqpConfig: amqplib.Options.Connect;
  private readonly retryTimeout: number;
  private readonly debug: boolean;
  private readonly exchangeConfig: ExchangeConfig;

  private connection: amqplib.Connection;
  private channel: amqplib.Channel;
  private hasBeenConnected: boolean;
  private isReconnecting: boolean;
  private subscriptions: Map<string, Array<CallbackFunc>>;
  private queue: PQueue;

  constructor(params: RabbitMQConstructorParams) {
    this.amqpConfig = params.amqp !== undefined ? params.amqp : {};
    this.retryTimeout = params.retryTimeout !== undefined ? params.retryTimeout : 10000;
    this.debug = params.debug !== undefined ? params.debug : true;

    if (params.exchange.type === ExchangeType.Direct) {
      assert(
        typeof params.exchange.name === 'string' &&
        params.exchange.name.length > 0,
        'Direct exchange requires a name',
      );
    }

    this.exchangeConfig = params.exchange;

    this.hasBeenConnected = false;
    this.isReconnecting = false;
    this.subscriptions = new Map<string, Array<CallbackFunc>>();
    this.queue = new PQueue({ concurrency: 1 });
  }

  async connect(): Promise<void> {
    await this.setupConnection();

    this.hasBeenConnected = true;
  }

  publish(namespace: string, data: DataPayload) {
    assert(this.hasBeenConnected, 'You must connect() first!');

    this.publishSynchronously(namespace, data);
  }

  async subscribe(namespace: string, callback: CallbackFunc) {
    assert(this.connection, 'You must connect() first!');

    await this.saveSubscription(namespace, callback);
    await this.createExchange(namespace);
    await this.createQueueAndBindItToExchange(namespace, callback);
  }

  private async setupConnection() {
    try {
      this.connection = await amqplib.connect(this.amqpConfig);
      this.channel = await this.connection.createChannel();
    } catch (err) {
      this.logError(err);
      throw new MQConnectionError(err.message);
    }

    this.connection.off('error', this.handleConnectionCloseOrError);
    this.connection.on('error', this.handleConnectionCloseOrError);

    this.connection.off('close', this.handleConnectionCloseOrError);
    this.connection.on('close', this.handleConnectionCloseOrError);
  }

  private handleConnectionCloseOrError = async (err: any) => {
    if (this.isReconnecting) {
      return;
    }

    this.isReconnecting = true;
    this.logError(err);
    this.connection = null;
    this.channel = null;

    await this.setupConnectionLoop();

    for (const namespace of this.subscriptions.keys()) {
      const callbacks = this.subscriptions.get(namespace);
      for (const callback of callbacks) {
        await this.createQueueAndBindItToExchange(namespace, callback);
      }
    }

    this.isReconnecting = false;
  };

  private async setupConnectionLoop(): Promise<void> {
    try {
      await this.setupConnection();
    } catch (err) {
      this.logError(err);
      await this.sleep(this.retryTimeout);
      return this.setupConnectionLoop();
    }
  }

  private async publishSynchronously(namespace: string, data: any) {
    const tryToSendMessageLoop = async (): Promise<void> => {
      if (this.channel) {
        try {
          await this.createExchange(namespace);
          await this.publishDataInExchange(data, namespace);

          return;
        } catch (err) {
          this.logError(err);
        }
      }

      await this.sleep(this.retryTimeout);
      return tryToSendMessageLoop();
    };

    await this.queue.add(tryToSendMessageLoop);
  }

  private saveSubscription(namespace: string, callback: CallbackFunc) {
    if (!this.subscriptions.has(namespace)) {
      this.subscriptions.set(namespace, []);
    }

    this.subscriptions.set(namespace, [
      ...this.subscriptions.get(namespace),
      callback,
    ]);
  }

  private async createExchange(exchange: string) {
    const config = {
      durable: this.exchangeConfig.durable !== undefined ? this.exchangeConfig.durable : true,
    };

    if (this.isExchangeInDirectType()) {
      await this.channel.assertExchange(this.exchangeConfig.name, ExchangeType.Direct, config);
    } else {
      await this.channel.assertExchange(exchange, ExchangeType.Fanout, config);
    }
  }

  private isExchangeInDirectType() {
    return this.exchangeConfig.type === ExchangeType.Direct;
  }

  private async publishDataInExchange(data: any, namespace: string) {
    if (this.isExchangeInDirectType()) {
      await this.channel.publish(this.exchangeConfig.name, namespace, this.toBuffer(data));
    } else {
      await this.channel.publish(namespace, '', this.toBuffer(data));
    }
  }

  private sleep(msec: number) {
    return new Promise((resolve) => setTimeout(resolve, msec));
  }

  private async createQueueAndBindItToExchange(namespace: string, callback: Function) {
    const queue = await this.channel.assertQueue('', { exclusive: true });

    if (this.isExchangeInDirectType()) {
      await this.channel.bindQueue(queue.queue, this.exchangeConfig.name, namespace);
    } else {
      await this.channel.bindQueue(queue.queue, namespace, '');
    }

    await this.channel.consume(queue.queue, async (msg) => {
      let parsed;
      try {
        parsed = this.parseBuffer(msg.content);
      } catch (err) {
        this.logError(err);
        return;
      }

      callback(parsed);
    }, { noAck: true });
  }

  private toBuffer(data: any) {
    return Buffer.from(JSON.stringify(data));
  }

  private parseBuffer(buffer: Buffer) {
    return JSON.parse(buffer.toString());
  }

  private logError(err: Error) {
    if (this.debug) {
      console.error(`[${this.constructor.name}]`, err);
    }
  }
}

export {
  RabbitMQClient,
  RabbitMQConstructorParams, // eslint-disable-line no-undef
};
