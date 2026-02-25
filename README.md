Redis Streams Bus
=================

This library enables you to use [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) as a message bus or queue.

At the technical level, Redis Streams Bus is a collection of Redis streams, each designated for a specific subject or message type, along with abstractions for working with them.


## Features

* Handle multiple message types and subjects within a single abstraction
* Independent consumption via consumer groups with any required number of consumers
* Ordered consumption within each subject
* Recover consumption after failures
* Delivery attempt counter
* Place unprocessed messages into a DLQ (Dead Letter Queue)
* Delay processing (NACK) for an arbitrary period in case of failure
* Set a maximum time for message processing
* Queue mode operation with deletion of processed items
* Limit the time and number of stored messages
* Tools for monitoring consumption and bus state


## Requirements

* **PHP** >= 8.2
* **Redis** or **Valkey** >= 7.2


## Installation

```shell
composer require alsc/streambus
```


## Usage

The library provides a [factory](src/StreamBusBuilder.php) for configuring and creating the necessary classes.
The main components and settings are described below.
You can find more usage examples in the [examples](examples) folder and in the [tests](tests) of the repository.

### Settings

Class `StreamBus\StreamBusSettings`

This class defines the settings for the entire message bus and is used during initialization of `StreamBusBuilder` and `StreamBus`.

| Key                | Description                                                                                                   | Default               |
|--------------------|---------------------------------------------------------------------------------------------------------------|-----------------------|
| minTTLSec          | Minimum message TTL in stream                                                                                 | `86400`               |
| maxSize            | Maximum messages stored per subject                                                                           | `1000000`             |
| exactLimits        | Apply exact limits [detail](https://redis.io/docs/latest/commands/xadd/#capped-streams)                       | `false`               |
| deleteOnAck        | Delete message from stream on ACK                                                                             | `false`               |
| deletePolicy       | How entries are removed when trimmed or acked with `deleteOnAck`. `KeepRef` / `DelRef` / `Acked` (Redis 8.2+) | `DeleteMode::KeepRef` |
| maxDelivery        | Maximum delivery attempts per message. `0` means no limit                                                     | `0`                   |
| ackExplicit        | Require explicit ACK from consumers                                                                           | `true`                |
| ackWaitMs          | Maximum time to process a message before it is redelivered                                                    | `30 * 60 * 1000`      |
| nackDelayMs        | Minimum delay before a NACKed message is redelivered                                                          | `0`                   |
| idmpMode           | Idempotent publishing mode. `None` / `Auto` / `Explicit` (Redis 8.6+)                                         | `IdmpMode::None`      |
| idmpDurationSec    | How long idempotency keys are retained. `0` uses the server default                                           | `0`                   |
| idmpMaxSize        | Maximum number of idempotency keys retained per stream. `0` uses the server default                           | `0`                   |
| maxExpiredSubjects | Maximum subjects scanned for expired messages per call. `0` means no limit                                    | `0`                   |

**Example**

```php
$settings = new StreamBusSettings(
    minTTLSec: 86_400,
    maxSize: 10_000_000,
    exactLimits: false,
    deleteOnAck: false,
    maxDelivery: 10,
    ackWaitMs: 30 * 60 * 1000,
    //...
);

$builder = StreamBusBuilder::create('bus_name')
    ->withSettings($settings)
    //...
```

### Produce

Class `StreamBus\Producer\StreamBusProducerInterface`

You can add messages to the bus either one by one or in batches.

**Example**
```php
$serializers = [
    'users.new' => new StreamBusJsonSerializer(),
    'products.new' => new StreamBusJsonSerializer(),
];

$builder = StreamBusBuilder::create('bus_name')
    ->withClient($redisClient)
    ->withSettings(new StreamBusSettings())
    ->withSerializers($serializers);

$producer = $builder->createProducer('producer');
$producer->add('users.new', ['id' => 1, 'name' => 'David']);
$producer->add('users.new', ['id' => 2, 'name' => 'Andrew']);
$producer->add('products.new', ['id' => 1, 'product' => 'guitar']);
$producer->addMany('products.new', [
    ['id' => 1, 'product' => 'guitar']
    ['id' => 2, 'product' => 'flute']
]);
```

### Consume

Class: `StreamBus\Consumer\StreamBusConsumerInterface`

There are several types of consumers for message consumption:
* `StreamBusConsumer` - no order guaranties, at least once delivery
* `StreamBusOrderedConsumer` - ordered per subject, at least once delivery 
* `StreamBusOrderedStrictConsumer` - same as previous with additional consistency checks

You can specify the subjects you are interested in when creating the consumers.
Consumers support blocking reads.

**Example**

```php
$serializers = [
    'users.new' => new StreamBusJsonSerializer(),
    'products.new' => new StreamBusJsonSerializer(),
    'orders.new' => new StreamBusJsonSerializer(),
];

$builder = StreamBusBuilder::create('bus_name')
    ->withClient($redisClient)
    ->withSettings(new StreamBusSettings())
    ->withSerializers($serializers);

$consumer = $builder->createConsumer('all', 'consumer');
// or
$consumer = $builder->createOrderedConsumer('users', ['users.new']);
// or
$consumer = $builder->createOrderedStrictConsumer('users_and_orders', ['users.new', 'orders.new']);

// Read max 5 messages per subject, block read for 10 seconds if no messages available to read
while ($messages = $consumer->read(5, 10_000)) {
    foreach ($messages as $subject => $subjectMessages) {
        foreach ($subjectMessages as $messageId => $message) {
            printf('got message from subject: %s, with id: %s' . PHP_EOL, $subject, $messageId);
            print_r($message);
            // ack
            $consumer->ack($subject, $messageId);
            // or nack
            $consumer->nack($subject, $messageId);
            // or nack with 10 seconds redelivery delay
            $consumer->nack($subject, $messageId, 10_000);
        }
    }
}
```

### Consume with processor

Class `StreamBus\Processor\StreamBusProcessor`

You can also process messages using the processor.

**Example**

```php
class User
{
    public function __construct(public int $id, public string $name) {}

    public function toArray(): array
    {
        return ['id' => $this->id, 'name' => $this->name];
    }

    public static function fromArray(array $data): self
    {
        return new self($data['id'], $data['name']);
    }
}

$builder = StreamBusBuilder::create('bus_name')
    ->withClient($redisClient)
    ->withSettings(new StreamBusSettings())
    ->withSerializers([
        'users.new' => new StreamBusJsonSerializer(
            static fn(User $user) => $user->toArray(),
            static fn(array $data) => User::fromArray($data),
        ),
    ]);

class UsersHandler
{
    public function __invoke(string $type, string $id, User $user): true
    {
        printf('Welcome, %s!' . PHP_EOL, $user->name);
        return true;
    }
}

$processor = $builder->createProcessor(
    'processor',
    'consumer',
    ['users.new' => new UsersHandler()]
)->process();
```

### Dead Letter Queue

The bus supports working with a [DLQ](https://en.wikipedia.org/wiki/Dead_letter_queue), placing messages there after `maxDelivery` delivery attempts.
An example of usage can be found in the [examples](examples/dlq.php) folder.

### Observe

Class: `StreamBus\StreamBus\StreamBusInfoInterface`

Using this interface, you can obtain:
* A list of existing subjects
* A list of consumer groups for a specific subject
* The number of messages taken for processing
* The consumer group lag
* Raw information about the state of the underlying Redis streams ([details](https://redis.io/docs/latest/commands/xinfo-stream/))

**Example**
```php
$info = $builder->createBusInfo();

foreach ($info->getSubjects() as $subject) {
    printf('Subject: %s' . PHP_EOL, $subject);
    printf('  Stream length: %d' . PHP_EOL, $info->getStreamLength($subject));
    foreach ($info->getGroups($subject) as $group) {
        printf('  Group: %s' . PHP_EOL, $group);
        printf('    Pending: %d' . PHP_EOL, $info->getGroupPending($group, $subject));
        printf('    Time lag: %d' . PHP_EOL, $info->getGroupPending($group, $subject));
    }
}
```


## Benchmark

The project includes [benchmarks](tests/Benchmark) that you can run. It is recommended to adjust the test file configurations according to your hardware.

My 5-year-old home laptop is capable of processing around 100,000 messages per second in batch mode.
On servers, I have achieved results of around 1 million messages per second in batch mode.


```shell
composer test:benchmark
```


## Contributing

This project is open source and welcomes contributions from the community. If you have ideas, improvements, or bug fixes, feel free to open an issue or submit a pull request!
