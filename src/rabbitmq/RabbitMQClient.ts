import assert from 'assert';
import amqplib from 'amqplib';
import PQueue from 'p-queue';

import { MQClient, DataPayload, CallbackFunc } from '../MQClient';
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

interface QueueConfig {
  name: string;
  exclusive: boolean;
}

interface ConstructorParams {
  amqp?: amqplib.Options.Connect;
  exchange: ExchangeConfig;
  queue?: QueueConfig;
  retryTimeout?: number;
  debug?: boolean;
}

class RabbitMQClient implements MQClient {
  private readonly amqpConfig: amqplib.Options.Connect;
  private readonly retryTimeout: number;
  private readonly debug: boolean;
  private readonly exchangeConfig: ExchangeConfig;
  private readonly queueConfig: QueueConfig;

  private connection: amqplib.Connection;
  private channel: amqplib.Channel;
  private hasBeenConnected: boolean;
  private subscriptions: Map<string, Array<CallbackFunc>>;
  private queue: PQueue;
  private setupConnectionPromise: Promise<void>;
  private hasManuallyUnsubscribed: boolean;

  constructor(params: ConstructorParams) {
    this.amqpConfig = params.amqp !== undefined ? params.amqp : {};
    this.retryTimeout = params.retryTimeout !== undefined ? params.retryTimeout : 1000;
    this.debug = params.debug !== undefined ? params.debug : true;

    if (params.exchange.type === ExchangeType.Direct) {
      assert(
        typeof params.exchange.name === 'string' &&
        params.exchange.name.length > 0,
        'Direct exchange requires a name',
      );
    }

    this.exchangeConfig = params.exchange;

    this.queueConfig = params.queue ?? {
      name: '',
      exclusive: true,
    };

    this.hasBeenConnected = false;
    this.subscriptions = new Map<string, Array<CallbackFunc>>();
    this.queue = new PQueue({ concurrency: 1 });
    this.setupConnectionPromise = null;
    this.hasManuallyUnsubscribed = false;
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

    const subscriptionKey = this.isExchangeInDirectType()
      ? `${this.exchangeConfig.name}#${namespace}`
      : namespace;

    await this.saveSubscription(subscriptionKey, callback);
    await this.createExchange(namespace);
    await this.createQueueAndBindItToExchange(namespace);
  }

  async unsubscribe() {
    assert(this.connection, 'You must connect() first!');
    assert(this.subscriptions.size, 'You must subscribe() first!');

    this.hasManuallyUnsubscribed = true;
    await this.channel.close();
    await this.connection.close();
  }

  private async setupConnection(): Promise<void> {
    if (this.setupConnectionPromise) {
      return this.setupConnectionPromise;
    }

    const setupConnectionLoop = async (): Promise<void> => {
      try {
        await this.createConnectionAndChannel();
        this.bindListeners();
        await this.recreateSubscriptions();
      } catch (err) {
        this.logError(err);
        await this.sleep(this.retryTimeout);
        return setupConnectionLoop();
      }
    };

    // eslint-disable-next-line no-async-promise-executor
    this.setupConnectionPromise = new Promise(async (resolve) => {
      await setupConnectionLoop();
      this.setupConnectionPromise = null;
      resolve();
    });

    return this.setupConnectionPromise;
  }

  private async createConnectionAndChannel() {
    this.connection = await amqplib.connect(this.amqpConfig);
    this.channel = await this.connection.createChannel();
  }

  private bindListeners() {
    this.connection.off('error', this.handleConnectionOrChannelProblem);
    this.connection.on('error', this.handleConnectionOrChannelProblem);
    this.connection.off('close', this.handleConnectionOrChannelProblem);
    this.connection.on('close', this.handleConnectionOrChannelProblem);

    this.channel.off('error', this.handleConnectionOrChannelProblem);
    this.channel.on('error', this.handleConnectionOrChannelProblem);
    this.channel.off('close', this.handleConnectionOrChannelProblem);
    this.channel.on('close', this.handleConnectionOrChannelProblem);
  }

  private handleConnectionOrChannelProblem = async (err?: Error) => {
    if (this.hasManuallyUnsubscribed) {
      return;
    }

    if (err) {
      this.logError(err);
    }

    await this.setupConnection();
  };

  private async recreateSubscriptions() {
    for (const [namespace] of this.subscriptions) {
      await this.createExchange(namespace);
      await this.createQueueAndBindItToExchange(namespace);
    }
  }

  private async publishSynchronously(namespace: string, data: any) {
    const tryToSendMessageLoop = async (): Promise<void> => {
      try {
        await this.publishDataInExchange(data, namespace);

        return;
      } catch (err) {
        this.logError(err);
        await this.setupConnection();
        await this.createExchange(namespace);
      }

      return tryToSendMessageLoop();
    };

    await this.queue.add(tryToSendMessageLoop);
  }

  private saveSubscription(key: string, callback: CallbackFunc) {
    const callbacks = this.subscriptions.has(key)
      ? this.subscriptions.get(key)
      : [];
    callbacks.push(callback);

    this.subscriptions.set(key, callbacks);
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

  private async createQueueAndBindItToExchange(namespace: string) {
    const queue = await this.channel.assertQueue(this.queueConfig.name, { exclusive: this.queueConfig.exclusive });

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

      const subscriptionKey = this.isExchangeInDirectType()
        ? `${msg.fields.exchange}#${msg.fields.routingKey}`
        : msg.fields.exchange;

      const callbacks = this.subscriptions.get(subscriptionKey);
      if (callbacks && callbacks.length) {
        for (const callback of callbacks) {
          callback(parsed);
        }
      }
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
  // eslint-disable-next-line no-undef
  ConstructorParams as RabbitMQClientConstructorParams,
};
