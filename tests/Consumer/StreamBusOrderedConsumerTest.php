<?php

namespace StreamBus\Consumer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBus\StreamBus;
use StreamBus\StreamBus\StreamBusSettings;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;

#[CoversClass(StreamBusOrderedConsumer::class)]
class StreamBusOrderedConsumerTest extends TestCase
{
    private StreamBus $bus;
    private StreamBusOrderedConsumer $group1consumer1;
    private StreamBusOrderedConsumer $group1consumer1Clone;

    protected function setUp(): void
    {
        $client = TestFactory::createClient();
        $client->flushdb();

        $settings = new StreamBusSettings(
            minTTLSec: 0,
            maxSize: 100,
            exactLimits: true,
            deleteOnAck: true,
            maxDelivery: 10,
            ackWaitMs: 30 * 60 * 1000,
        );

        $builder = StreamBusBuilder::create('test')
            ->withClient($client)
            ->withSettings($settings)
            ->withSerializers([
                'subject_a' => new StreamBusJsonSerializer(),
                'subject_b' => new StreamBusJsonSerializer(),
            ]);

        $this->bus = $builder->createBus();
        $this->group1consumer1 = $builder->createOrderedConsumer('group1', ['subject_a', 'subject_b']);
        $this->group1consumer1Clone = clone $this->group1consumer1;
    }

    public function testAck(): void
    {
        $id1 = $this->bus->add('subject_a', ['k1' => 'v1']);
        $this->group1consumer1->read();
        $this->group1consumer1->ack('subject_a', $id1);
        $this->assertSame([], $this->group1consumer1Clone->read());
    }

    public function testRead(): void
    {
        $id1 = $this->bus->add('subject_a', ['k1' => 'v1']);
        $id2 = $this->bus->add('subject_a', ['k2' => 'v2']);
        $this->group1consumer1Clone->read();
        $this->assertSame(['subject_a' => [$id1 => ['k1' => 'v1']]], $this->group1consumer1->read());
        $this->assertSame(['subject_a' => [$id2 => ['k2' => 'v2']]], $this->group1consumer1->read());
    }

    public function testNackThrows(): void
    {
        $this->expectException(StreamBusConsumerException::class);
        $this->group1consumer1->nack('subject_a', '0-0');
    }
}
