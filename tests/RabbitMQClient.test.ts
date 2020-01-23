import {
  mockAmqplib,
  mockChannel,
  mockConnection,

  createMockQueue,
  triggerMockConnectionListener,
  triggerMockChannelConsumer,
  resetMocks,
} from './mocks';

import { AssertionError } from 'assert';
import {
  RabbitMQClient,
  RabbitMQConstructorParams,
  MQConnectionError,
} from '../src/index';

const sleep = (msec: number) => new Promise((resolve) => setTimeout(resolve, msec));

// eslint-disable-next-line @typescript-eslint/no-empty-function
const noop = () => {};

describe('RabbitMQClient', () => {
  let client: RabbitMQClient;
  describe('Fanout specs', () => {
    const defaultConstructorParams: RabbitMQConstructorParams = {
      retryTimeout: 100,
      exchange: {
        type: 'fanout',
      },
    };

    beforeEach(() => {
      resetMocks();
      client = new RabbitMQClient(defaultConstructorParams);
    });

    const testRetryTimeout = defaultConstructorParams.retryTimeout + 200;

    it('should throw AssertionError when constructing with with direct exchange and not supplying the name of the exchange', async () => {
      const params: RabbitMQConstructorParams = {
        amqp: {},
        retryTimeout: 10,
        exchange: {
          type: 'direct',
        },
      };

      expect(() => new RabbitMQClient(params)).toThrow(AssertionError);
    });

    it('should throw AssertionError when constructing with with direct exchange and supplying empty exchange', async () => {
      const params: RabbitMQConstructorParams = {
        amqp: {},
        retryTimeout: 10,
        exchange: {
          type: 'direct',
          name: '',
        },
      };

      expect(() => new RabbitMQClient(params)).toThrow(AssertionError);
    });

    it('should call the amqplib.connect method with the correct opts from the constructor', async () => {
      const params: RabbitMQConstructorParams = {
        amqp: {
          protocol: 'p',
          hostname: 'h',
          port: 1,
          username: 'u',
          password: 'p',
          locale: 'l',
          frameMax: 1,
          heartbeat: 1,
          vhost: '',
        },
        retryTimeout: 10,
        exchange: {
          type: 'fanout',
        },
      };

      const customClient = new RabbitMQClient(params);
      await customClient.connect();

      expect(mockAmqplib.connect).toHaveBeenCalledWith(params.amqp);
    });

    it('should throw MQConnectionError when amqplib.connect throws', async () => {
      mockAmqplib.connect.mockRejectedValueOnce(new Error('Failed to connect!'));

      await expect(client.connect()).rejects.toThrow(MQConnectionError);
    });

    it('should throw MQConnectionError when connection.createChannel throws', async () => {
      mockConnection.createChannel.mockRejectedValueOnce(new Error('Failed to create channel!'));

      await expect(client.connect()).rejects.toThrow(MQConnectionError);
    });

    it('should throw AssertionError when calling it without connecting first', async () => {
      expect(() => client.publish('namespace', 1)).toThrow(AssertionError);
    });

    it('should create a durable fanout exchange when calling with a name equal to the namespace', async () => {
      await client.connect();
      client.publish('my-namespace', 1);
      await sleep(testRetryTimeout);

      expect(mockChannel.assertExchange).toHaveBeenCalledWith('my-namespace', 'fanout', { durable: true });
    });

    it('should publish a message with empty routingKey in the newly created exchange', async () => {
      await client.connect();
      client.publish('my-namespace', 1);
      await sleep(100);

      expect(mockChannel.publish).toHaveBeenCalledWith('my-namespace', '', expect.anything());
    });

    it('should publish the message as a buffer with payload of the json serialized message', async () => {
      await client.connect();

      const dummyPayload = { bar: 'foo' };
      client.publish('my-namespace', dummyPayload);
      await sleep(testRetryTimeout);

      expect(mockChannel.publish).toHaveBeenCalledWith(expect.anything(), expect.anything(), Buffer.from(JSON.stringify(dummyPayload)));
    });

    it('should retry to send the message when the connection is down', async () => {
      await client.connect();

      mockConnection.createChannel.mockRejectedValueOnce(new Error('Failed to create channel!'));
      triggerMockConnectionListener('error', new Error('Something went wrong'));

      const dummyPayload = { bar: 'foo' };
      client.publish('my-namespace', dummyPayload);
      await sleep(testRetryTimeout);

      expect(mockChannel.publish).toHaveBeenCalledWith(expect.anything(), expect.anything(), Buffer.from(JSON.stringify(dummyPayload)));
    });

    it('should return undefined', async () => {
      await client.connect();

      expect(client.publish('my-namespace', 1)).toBeUndefined();
    });

    it('should automatically attempt to reconnect in case the connection is dropped', async () => {
      await client.connect();

      triggerMockConnectionListener('error', new Error('Something went wrong'));
      await sleep(testRetryTimeout);

      expect(mockAmqplib.connect).toHaveBeenCalledTimes(2);
      expect(mockConnection.createChannel).toHaveBeenCalledTimes(2);
    });

    it('should throw AssertionError when calling subscribe before connecting', async () => {
      await expect(client.subscribe('namespace', noop)).rejects.toThrow(AssertionError);
    });

    it('should create a durable fanout exchange for the particular namespace when calling subscribe', async () => {
      await client.connect();
      await client.subscribe('my-namespace', noop);

      expect(mockChannel.assertExchange).toHaveBeenCalledWith('my-namespace', 'fanout', { durable: true });
    });

    it('should create an anonymous queue when calling subscribe', async () => {
      await client.connect();
      await client.subscribe('my-namespace', noop);

      expect(mockChannel.assertQueue).toHaveBeenCalledWith('', { exclusive: true });
    });

    it('should bind the anonymous queue to the exchange using empty routingKey when calling subscribe', async () => {
      const mockQueue = createMockQueue();
      mockChannel.assertQueue.mockResolvedValueOnce(mockQueue);

      await client.connect();
      await client.subscribe('my-namespace', noop);

      expect(mockChannel.bindQueue).toHaveBeenCalledWith(mockQueue.queue, 'my-namespace', '');
    });

    it('should consume the anonymous queue when calling subscribe', async () => {
      const mockQueue = createMockQueue();
      mockChannel.assertQueue.mockResolvedValueOnce(mockQueue);

      await client.connect();
      await client.subscribe('my-namespace', noop);

      expect(mockChannel.consume).toHaveBeenCalledWith(mockQueue.queue, expect.anything(), { noAck: true });
    });

    it('should invoke the callback passed to subscribe when a message is received', async () => {
      const mockQueue = createMockQueue();
      mockChannel.assertQueue.mockResolvedValueOnce(mockQueue);

      await client.connect();

      const mockCallback = jest.fn();
      const dummyData = { bar: 'foo' };
      await client.subscribe('my-namespace', mockCallback);

      triggerMockChannelConsumer(mockQueue.queue, dummyData);
      await sleep(testRetryTimeout);

      expect(mockCallback).toHaveBeenCalledWith(dummyData);
    });

    it('should not kill the process when the message fails to be parsed', async () => {
      const mockQueue = createMockQueue();
      mockChannel.assertQueue.mockResolvedValueOnce(mockQueue);

      await client.connect();

      const mockCallback = jest.fn();
      await client.subscribe('my-namespace', mockCallback);

      const dummyData = 'not-a-valid-json';
      triggerMockChannelConsumer(mockQueue.queue, dummyData, false);
      await sleep(testRetryTimeout);
    });

    it('should handle client callback errors gracefully', async () => {
      const mockQueue = createMockQueue();
      mockChannel.assertQueue.mockResolvedValueOnce(mockQueue);

      await client.connect();

      const mockCallback = jest.fn();
      mockCallback.mockRejectedValue(new Error('We fucked up!'));
      await client.subscribe('my-namespace', mockCallback);

      const dummyData = { bar: 'foo' };
      triggerMockChannelConsumer(mockQueue.queue, dummyData);
      await sleep(testRetryTimeout);
    });

    it('should recreate the previously created queues on reconnect', async () => {
      await client.connect();
      await client.subscribe('my-namespace', noop);
      await client.subscribe('my-namespace', noop);

      triggerMockConnectionListener('error', new Error('Something went wrong'));
      await sleep(testRetryTimeout);

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(4);
      expect(mockChannel.assertQueue).toHaveBeenNthCalledWith(3, '', { exclusive: true });
      expect(mockChannel.assertQueue).toHaveBeenNthCalledWith(4, '', { exclusive: true });
    });

    it('should bind the newly created queues to the exchange', async () => {
      await client.connect();
      await client.subscribe('my-namespace', noop);

      const mockQueue = createMockQueue('queue-2.0');
      mockChannel.assertQueue.mockResolvedValue(mockQueue);
      triggerMockConnectionListener('error', new Error('Something went wrong'));
      await sleep(testRetryTimeout);

      expect(mockChannel.bindQueue).toHaveBeenCalledTimes(2);
      expect(mockChannel.bindQueue).toHaveBeenNthCalledWith(2, mockQueue.queue, 'my-namespace', '');
    });


    it('should consume the newly recreated queues', async () => {
      await client.connect();

      const mockCallback = jest.fn();
      await client.subscribe('my-namespace', mockCallback);

      const mockQueue = createMockQueue('queue-2.0');
      mockChannel.assertQueue.mockResolvedValue(mockQueue);

      triggerMockConnectionListener('error', new Error('Something went wrong'));
      await sleep(testRetryTimeout);

      expect(mockChannel.consume).toHaveBeenCalledTimes(2);
      expect(mockChannel.consume).toHaveBeenNthCalledWith(2, mockQueue.queue, expect.anything(), { noAck: true });
    });

    it('should invoke the callback passed to subscribe when a message is received after a reconnect', async () => {
      await client.connect();

      const mockCallback = jest.fn();
      await client.subscribe('my-namespace', mockCallback);

      const mockQueue = createMockQueue('queue-2.0');
      mockChannel.assertQueue.mockResolvedValue(mockQueue);

      triggerMockConnectionListener('error', new Error('Something went wrong'));
      await sleep(testRetryTimeout);

      const dummyData = {};
      triggerMockChannelConsumer(mockQueue.queue, dummyData);
      await sleep(testRetryTimeout);

      expect(mockCallback).toHaveBeenCalledWith(dummyData);
    });

    it('should not invoke the callback passed to subscribe when a maformed message is received after a reconnect', async () => {
      await client.connect();

      const mockCallback = jest.fn();
      await client.subscribe('my-namespace', mockCallback);

      const mockQueue = createMockQueue('queue-2.0');
      mockChannel.assertQueue.mockResolvedValue(mockQueue);

      triggerMockConnectionListener('error', new Error('Something went wrong'));
      await sleep(testRetryTimeout);

      const dummyData = 'not-a-valid-json';
      triggerMockChannelConsumer(mockQueue.queue, dummyData, false);
      await sleep(testRetryTimeout);

      expect(mockCallback).not.toHaveBeenCalled();
    });

    it('should handle client callback errors gracefully after reconnect', async () => {
      await client.connect();

      const errorThrowingCallback = () => {
        throw new SyntaxError('I fucked up.');
      };
      await client.subscribe('my-namespace', errorThrowingCallback);

      const mockQueue = createMockQueue('queue-2.0');
      mockChannel.assertQueue.mockResolvedValue(mockQueue);

      triggerMockConnectionListener('error', new Error('Something went wrong'));
      await sleep(testRetryTimeout);

      triggerMockChannelConsumer(mockQueue.queue, 'asd');
      await sleep(testRetryTimeout);
    });
  });

  describe('Direct specs', () => {
    const defaultConstructorParams: RabbitMQConstructorParams = {
      retryTimeout: 100,
      exchange: {
        type: 'direct',
        name: 'direct-exchange',
      },
    };

    const testRetryTimeout = defaultConstructorParams.retryTimeout + 200;

    beforeEach(() => {
      resetMocks();
      client = new RabbitMQClient(defaultConstructorParams);
    });

    it('should create a durable direct exchange with the name provided inside the construction params', async () => {
      await client.connect();
      await client.publish('my-nsp', 'whatever');
      await sleep(testRetryTimeout);

      expect(mockChannel.assertExchange).toHaveBeenCalledWith('direct-exchange', 'direct', { durable: true });
    });

    it('should publish a message with the namespace as routingKey in the newly created direct exchange', async () => {
      await client.connect();
      client.publish('my-namespace', 1);
      await sleep(100);

      expect(mockChannel.publish).toHaveBeenCalledWith('direct-exchange', 'my-namespace', expect.anything());
    });

    it('should bind the anonymous queue to the direct exchange using the namespace as routingKey when calling subscribe', async () => {
      const mockQueue = createMockQueue();
      mockChannel.assertQueue.mockResolvedValueOnce(mockQueue);

      await client.connect();
      await client.subscribe('my-namespace', noop);

      expect(mockChannel.bindQueue).toHaveBeenCalledWith(mockQueue.queue, 'direct-exchange', 'my-namespace');
    });
  });
});

