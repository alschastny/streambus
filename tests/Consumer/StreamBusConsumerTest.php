<?php

declare(strict_types=1);

namespace StreamBus\Consumer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBus\StreamBus;
use StreamBus\StreamBus\StreamBusSettings;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;

#[CoversClass(StreamBusConsumer::class)]
class StreamBusConsumerTest extends TestCase
{
    private StreamBus $bus;
    private StreamBusConsumer $group1consumer1;
    private StreamBusConsumer $group1consumer1Clone;

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
        $this->group1consumer1 = $builder->createConsumer('group1', 'consumer1', ['subject_a', 'subject_b']);
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

    public function testAddNackRead(): void
    {
        $msg = ['k1' => 'v1'];
        $id = $this->bus->add('subject_a', $msg);
        $this->assertNotEmpty($id);
        $this->assertSame(['subject_a' => [$id => $msg]], $this->group1consumer1->read());
        $this->assertSame(1, $this->group1consumer1->nack('subject_a', $id));
        $this->assertSame(['subject_a' => [$id => $msg]], $this->group1consumer1->read());
    }
}
