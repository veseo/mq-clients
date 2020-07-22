[![Build Status](https://travis-ci.org/nikksan/mq-clients.svg?branch=message-replay)](https://travis-ci.org/nikksan/mq-clients)

[![Coverage Status](https://coveralls.io/repos/github/nikksan/mq-clients/badge.svg?branch=message-replay)](https://coveralls.io/github/nikksan/mq-clients?branch=message-replay)

# Message Broker Clients

A collection of message broker implementations.
Each implementation must conform the unified interface, which defines a single client who acts as both publisher and subscriber.

### Installing

```
npm install -s @luckbox/mq-clients
```

## Running the tests

```
npm run test
```

### Usage

```
import { RabbitMQClient } from '@luckbox/mq-clients';

const client = new RabbitMQClient({
  amqp: {},
  exchange: {
    type: 'fanout',
  },
  retryTimeout: 1000,
  debug: true,
});

client.connect().then(() => {
  client.subscribe('nsp', console.log);
  client.publish('nsp', 'data');
});
```
