<?php

namespace StreamBus\Producer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBus\StreamBusInfo;
use StreamBus\StreamBus\StreamBusSettings;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;

#[CoversClass(StreamBusProducer::class)]
class StreamBusProducerTest extends TestCase
{
    private StreamBusInfo $info;
    private StreamBusProducer $producer;

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

        $this->info = $builder->createBusInfo();
        $this->producer = $builder->createProducer('producer');
    }

    public function testAdd(): void
    {
        $this->producer->add('subject_a', ['k' => 'v']);
        $this->assertSame(1, $this->info->getStreamLength('subject_a'));
        $this->producer->add('subject_a', ['k' => 'v']);
        $this->producer->add('subject_a', ['k' => 'v']);
        $this->assertSame(3, $this->info->getStreamLength('subject_a'));
    }

    public function testAddMany(): void
    {
        $messages = array_fill(0, 5, ['k' => 'v']);
        $this->producer->addMany('subject_a', $messages);
        $this->assertSame(5, $this->info->getStreamLength('subject_a'));
        $this->producer->addMany('subject_a', $messages);
        $this->assertSame(10, $this->info->getStreamLength('subject_a'));
    }
}
