<?php

use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBus\StreamBusSettings;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;

require_once __DIR__ . '/../vendor/autoload.php';

$serializers = [
    'users.new' => new StreamBusJsonSerializer(),
    'products.new' => new StreamBusJsonSerializer(),
    'orders.new' => new StreamBusJsonSerializer(),
];

$settings = new StreamBusSettings(
    minTTLSec: 86_400,
    maxSize: 10_000_000,
    exactLimits: false,
    deleteOnAck: false,
    maxDelivery: 10,
    ackWaitMs: 30 * 60 * 1000,
);

$builder = StreamBusBuilder::create('basic')
    ->withClient(TestFactory::createClient())
    ->withSettings($settings)
    ->withSerializers($serializers);

// Produce messages
$producer = $builder->createProducer();
$producer->add('users.new', ['id' => 1, 'name' => 'David']);
$producer->add('users.new', ['id' => 2, 'name' => 'Andrew']);
$producer->add('products.new', ['id' => 1, 'product' => 'guitar']);
$producer->add('products.new', ['id' => 2, 'product' => 'flute']);
$producer->add('orders.new', ['id' => 1, 'user_id' => 1, 'products' => [1, 2]]);
$producer->add('orders.new', ['id' => 2, 'user_id' => 2, 'products' => [2, 1, 2]]);

// Consume all subjects
$consumer = $builder->createConsumer('all', 'consumer');
while ($messages = $consumer->read()) {
    foreach ($messages as $subject => $subjectMessages) {
        foreach ($subjectMessages as $messageId => $message) {
            printf('got message from subject: %s, with id: %s' . PHP_EOL, $subject, $messageId);
            print_r($message);
            $consumer->ack($subject, $messageId);
        }
    }
}

// Consume only users.new and orders.new subjects
$consumer = $builder->createConsumer('users_and_orders', 'consumer', ['users.new', 'orders.new']);
while ($messages = $consumer->read()) {
    foreach ($messages as $subject => $subjectMessages) {
        foreach ($subjectMessages as $messageId => $message) {
            printf('got message from subject: %s, with id: %s' . PHP_EOL, $subject, $messageId);
            print_r($message);
            $consumer->ack($subject, $messageId);
        }
    }
}

// Consume with processor
$processor = $builder->createProcessor(
    'processor',
    'consumer',
    [
        'users.new' => function (string $type, string $id, array $msg) {
            printf('Welcome, %s!' . PHP_EOL, $msg['name']);
            return true;
        },
        'products.new' => function (string $type, string $id, array $msg) {
            printf('New product has arrived: %s.' . PHP_EOL, $msg['product']);
            return true;
        },
        'orders.new' => function (string $type, string $id, array $msg) {
            printf('New order has been created with %d products.' . PHP_EOL, count($msg['products']));
            return true;
        },
    ],
)->process(5);
