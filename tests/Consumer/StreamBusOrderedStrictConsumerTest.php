<?php

namespace StreamBus\Consumer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBus\StreamBus;
use StreamBus\StreamBus\StreamBusSettings;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;

#[CoversClass(StreamBusOrderedStrictConsumer::class)]
class StreamBusOrderedStrictConsumerTest extends TestCase
{
    private StreamBus $bus;
    private StreamBusOrderedStrictConsumer $group1consumer1;
    private StreamBusOrderedStrictConsumer $group1consumer1Clone;

    protected function setUp(): void
    {
        $client = TestFactory::createClient();
        $client->flushdb();

        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 100,
            exactLimits: false,
            deleteOnAck: false,
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
        $this->group1consumer1 = $builder->createOrderedStrictConsumer('group1', ['subject_a', 'subject_b']);
        $this->group1consumer1Clone = clone $this->group1consumer1;
    }

    public function testNotAckThrows(): void
    {
        $id1 = $this->bus->add('subject_a', ['k1' => 'v1']);
        $this->group1consumer1->read();
        $this->assertSame(1, $this->group1consumer1Clone->ack('subject_a', $id1));
        $this->expectException(StreamBusConsumerException::class);
        $this->group1consumer1->ack('subject_a', $id1);
    }

    public function testNackThrows(): void
    {
        $this->expectException(StreamBusConsumerException::class);
        $this->group1consumer1->nack('subject_a', '0-0');
    }

    public function testPendingConsistencyThrows(): void
    {
        $id1 = $this->bus->add('subject_a', ['k1' => 'v1']);
        $id2 = $this->bus->add('subject_a', ['k2' => 'v2']);
        $this->group1consumer1Clone->read();
        $this->group1consumer1->read(1);
        $this->group1consumer1->read(1);
        $this->assertSame(1, $this->bus->ack('group1', 'subject_a', $id1));
        $this->expectException(StreamBusConsumerException::class);
        $this->group1consumer1->ack('subject_a', $id2);
    }

    public function testUnknownConsumer(): void
    {
        $this->bus->add('subject_a', ['k1' => 'v1']);
        $this->bus->createGroup('group1');
        $this->bus->readNew('group1', 'consumer_unknown', 1);
        $this->expectException(StreamBusConsumerException::class);
        $this->group1consumer1->read();
    }

    public function testUnknownConsumersWhileInit(): void
    {
        $this->bus->add('subject_a', ['k1' => 'v1']);
        $this->bus->createGroup('group1');
        $this->bus->readNew('group1', 'consumer_unknown1', 1);
        $this->bus->readNew('group1', 'consumer_unknown2', 1);
        $this->expectException(StreamBusConsumerException::class);
        $this->group1consumer1->read();
    }

    public function testUnknownConsumersWhileAck(): void
    {
        $id1 = $this->bus->add('subject_a', ['k1' => 'v1']);
        $this->group1consumer1->read();
        $this->bus->readNew('group1', 'consumer_unknown', 1);
        $this->expectException(StreamBusConsumerException::class);
        $this->group1consumer1->ack('subject_a', $id1);
    }
}
