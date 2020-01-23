import amqplib from 'amqplib';
jest.mock('amqplib');

const mockChannel = {} as jest.Mocked<amqplib.Channel>;

mockChannel.assertExchange = jest.fn();
mockChannel.publish = jest.fn();
mockChannel.assertQueue = jest.fn();
mockChannel.bindQueue = jest.fn();
mockChannel.consume = jest.fn();

let mockChannelConsumers: Array<any> = [];
mockChannel.consume.mockImplementation(function (queue: string, callback: Function) {
  mockChannelConsumers.push({ queue, callback });
  return this;
});

function triggerMockChannelConsumer(queue: string, data: any, serialize = true) {
  mockChannelConsumers
    .filter((consumer) => consumer.queue === queue)
    .forEach((consumer) => {
      const message = {} as jest.Mocked<amqplib.Message>;
      message.content = Buffer.from(serialize ? JSON.stringify(data) : data);
      consumer.callback(message);
    });
}

function resetMockChannelConsumers() {
  mockChannelConsumers = [];
}

const mockConnection = {} as jest.Mocked<amqplib.Connection>;
mockConnection.createChannel = jest.fn();
mockConnection.on = jest.fn();

let mockConnectionListeners: Array<any> = [];
mockConnection.on.mockImplementation(function (event: string, callback: Function) {
  mockConnectionListeners.push({ event, callback });
  return this;
});

mockConnection.off = jest.fn();
mockConnection.off.mockImplementation(function (event: string, callback: Function) {
  mockConnectionListeners = mockConnectionListeners.filter((listener) => !(listener.event === event && listener.callback === callback));
  return this;
});

function triggerMockConnectionListener(event: string, data: any) {
  mockConnectionListeners
    .filter((listener) => listener.event === event)
    .forEach((listener) => listener.callback(data));
}

function resetMockConnectionListeners() {
  mockConnectionListeners = [];
}

function createMockQueue(name = 'mock-queue') {
  const mockQueue = {
    queue: name,
  } as jest.Mocked<amqplib.Replies.AssertQueue>;

  return mockQueue;
}

const mockAmqplib = amqplib as jest.Mocked<typeof amqplib>;


function resetMocks() {
  mockAmqplib.connect.mockReset();
  mockConnection.createChannel.mockReset();

  mockChannel.assertExchange.mockReset();
  mockChannel.publish.mockReset();
  mockChannel.assertQueue.mockReset();
  mockChannel.bindQueue.mockReset();
  mockChannel.consume.mockClear();

  resetMockConnectionListeners();
  resetMockChannelConsumers();

  mockAmqplib.connect.mockResolvedValue(mockConnection);
  mockConnection.createChannel.mockResolvedValue(mockChannel);
  mockChannel.assertQueue.mockResolvedValue(createMockQueue());
}

export {
  mockAmqplib,
  mockChannel,
  mockConnection,
  triggerMockChannelConsumer,
  createMockQueue,
  triggerMockConnectionListener,
  resetMocks,
};
