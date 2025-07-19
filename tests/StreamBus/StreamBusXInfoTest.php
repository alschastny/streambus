<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;

#[CoversClass(StreamBusXInfo::class)]
class StreamBusXInfoTest extends TestCase
{
    private StreamBus $bus;
    private StreamBusInfo $info;

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
        $this->info = $builder->createBusInfo();
    }

    public function testXinfo(): void
    {
        $msg = ['k1' => 'v1'];
        $this->bus->add('subject_a', $msg);
        $this->bus->createGroup('group1');
        $this->bus->readNew('group1', 'consumer1', 1);

        $this->assertSame([], $this->info->xinfo('subject_unknown')->consumers('group1'));
        $this->assertNotEmpty($this->info->xinfo('subject_a')->consumers('group1'));

        $this->assertSame([], $this->info->xinfo('subject_unknown')->groups());
        $this->assertNotEmpty($this->info->xinfo('subject_a')->groups());

        $this->assertSame([], $this->info->xinfo('subject_unknown')->stream());
        $this->assertNotEmpty($this->info->xinfo('subject_a')->stream(true, 5));
    }
}
