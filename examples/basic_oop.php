<?php

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

class Product
{
    public function __construct(public int $id, public string $product) {}

    public function toArray(): array
    {
        return ['id' => $this->id, 'product' => $this->product];
    }

    public static function fromArray(array $data): self
    {
        return new self($data['id'], $data['product']);
    }
}

class Order
{
    public function __construct(public int $id, public int $userId, public array $products) {}

    public function toArray(): array
    {
        return ['id' => $this->id, 'user_id' => $this->userId, 'products' => $this->products];
    }

    public static function fromArray(array $data): self
    {
        return new self($data['id'], $data['user_id'], $data['products']);
    }
}

$serializers = [
    'users.new' => new StreamBusJsonSerializer(
        static fn(User $user) => $user->toArray(),
        static fn(array $data) => User::fromArray($data),
    ),
    'products.new' => new StreamBusJsonSerializer(
        static fn(Product $product) => $product->toArray(),
        static fn(array $data) => Product::fromArray($data),
    ),
    'orders.new' => new StreamBusJsonSerializer(
        static fn(Order $order) => $order->toArray(),
        static fn(array $data) => Order::fromArray($data),
    ),
];

$settings = new StreamBusSettings(
    minTTLSec: 86_400,
    maxSize: 10_000_000,
    exactLimits: false,
    deleteOnAck: false,
    maxDelivery: 10,
    ackWaitMs: 30 * 60 * 1000,
);

$builder = StreamBusBuilder::create('basic_oop')
    ->withClient(TestFactory::createClient())
    ->withSettings($settings)
    ->withSerializers($serializers);

// Produce messages
$producer = $builder->createProducer();
$producer->add('users.new', new User(1, 'David'));
$producer->add('users.new', new User(2, 'Andrew'));
$producer->add('products.new', new Product(1, 'guitar'));
$producer->add('products.new', new Product(2, 'flute'));
$producer->add('orders.new', new Order(1, 1, [1, 2]));
$producer->add('orders.new', new Order(2, 2, [2, 1, 2]));

// Handlers
class UsersHandler
{
    public function __invoke(string $type, string $id, User $user): true
    {
        printf('Welcome, %s!' . PHP_EOL, $user->name);
        return true;
    }
}

class ProductsHandler
{
    public function __invoke(string $type, string $id, Product $product): true
    {
        printf('New product has arrived: %s.' . PHP_EOL, $product->product);
        return true;
    }
}

class OrdersHandler
{
    public function __invoke(string $type, string $id, Order $order): true
    {
        printf('New order has been created with %d products.' . PHP_EOL, count($order->products));
        return true;
    }
}

// Consume
$processor = $builder->createProcessor(
    'processor',
    'consumer',
    [
        'users.new' => new UsersHandler(),
        'products.new' => new ProductsHandler(),
        'orders.new' => new OrdersHandler(),
    ],
)->process(2);
