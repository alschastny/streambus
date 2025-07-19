<?php

use StreamBus\Consumer\StreamBusConsumer;
use StreamBus\Processor\StreamBusProcessor;
use StreamBus\Producer\StreamBusProducer;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBus\StreamBusSettings;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;

require_once __DIR__ . '/../vendor/autoload.php';

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

$serializers = [
    'users.new' => new StreamBusJsonSerializer(
        static fn(User $user) => $user->toArray(),
        static fn(array $data) => User::fromArray($data),
    ),
];

$settings = new StreamBusSettings(
    minTTLSec: 3600,
    maxSize: 1000,
    maxDelivery: 2,
);

$dlqSettings = new StreamBusSettings(
    minTTLSec: 86_400,
    maxSize: 1000,
    deleteOnAck: true,
    maxDelivery: 0,
);

$baseBuilder = StreamBusBuilder::create('basic_dlq')
    ->withClient(TestFactory::createClient())
    ->withSerializers($serializers);
$dlq = $baseBuilder->withSettings($dlqSettings)->createDLQBus();
$dlqInfo = $baseBuilder->createDLQBusInfo();
$builder = $baseBuilder->withSettings($settings)->withDLQ($dlq);

// Produce messages
$producer = $builder->createProducer();
$producer->add('users.new', new User(1, 'David'));
$producer->add('users.new', new User(2, 'Andrew'));

// Handlers
class UsersBrokenHandler
{
    public function __invoke(string $subject, string $id, User $user): bool
    {
        printf('can\'t process message with id %s, so sorry %s' . PHP_EOL, $id, $user->name);
        return false;
    }
}

class UsersHandler
{
    public function __invoke(string $type, string $id, User $user): true
    {
        printf('Welcome, %s!' . PHP_EOL, $user->name);
        return true;
    }
}

class RedriveHandler
{
    public function __construct(
        private StreamBusProducer $producer,
    ) {}

    public function __invoke(string $subject, string $id, mixed $message): true
    {
        printf('push back to stream bus message with id %s' . PHP_EOL, $id);
        $this->producer->add($subject, $message);
        return true;
    }
}

// Consume with broken handler
$processor = $builder->createProcessor(
    'processor',
    'consumer',
    ['users.new' => new UsersBrokenHandler()],
);
$processor->process(10);

// Check our messages are in DLQ now
printf('now we have %d messages in DLQ' . PHP_EOL, $dlqInfo->getStreamLength('users.new'));

// Redrive them back to bus
$dlqConsumer = new StreamBusConsumer($dlq, 'redrive', 'consumer');
$dlqProcessor = (new StreamBusProcessor($dlqConsumer))
    ->setHandler('users.new', new RedriveHandler($producer));
$dlqProcessor->process(2);

// Now we can process them again with another handler
$processor = $builder->createProcessor(
    'processor',
    'consumer',
    ['users.new' => new UsersHandler()],
);
$processor->process(2);

printf('now we have %d messages in DLQ' . PHP_EOL, $dlqInfo->getStreamLength('users.new'));
