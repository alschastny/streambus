<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;

#[CoversClass(StreamBusInfo::class)]
class StreamBusInfoTest extends TestCase
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

    public function testGetSubjects(): void
    {
        $this->bus->createGroup('group1');
        $this->assertSame(['subject_a', 'subject_b'], $this->info->getSubjects());
    }

    public function testGetGroups(): void
    {
        $this->bus->createGroup('group1');
        $this->bus->createGroup('group2');
        $this->assertSame(['group1', 'group2'], $this->info->getGroups('subject_a'));
        $this->assertSame([], $this->info->getGroups('subject_unknown'));
    }

    public function testGroupPending(): void
    {
        $msg = ['k' => 'v'];
        $this->bus->addMany('subject_a', [$msg, $msg, $msg]);
        $this->bus->createGroup('group1');
        $this->bus->readNew('group1', 'consumer1', 1);
        $this->assertSame(1, $this->info->getGroupPending('group1', 'subject_a'));
        $this->bus->readNew('group1', 'consumer1', 2);
        $this->assertSame(3, $this->info->getGroupPending('group1', 'subject_a'));

        $this->assertSame(0, $this->info->getGroupPending('group1', 'subject_unknown'));
        $this->assertSame(0, $this->info->getGroupPending('group_unknown', 'subject_a'));
    }

    public function testGetGroupTimeLag(): void
    {
        $msg = ['k' => 'v'];
        $id = $this->bus->add('subject_a', $msg);
        $this->bus->createGroup('group1');
        $this->assertNotEmpty($id);
        $this->assertSame(
            ['subject_a' => [$id => $msg]],
            $this->bus->readNew('group1', 'consumer1', 1),
        );
        $this->assertThat(
            $this->info->getGroupTimeLag('group1', 'subject_a'),
            $this->logicalAnd(
                $this->greaterThanOrEqual(0),
                $this->lessThanOrEqual(1000),
            ),
        );
        sleep(1);
        $this->bus->add('subject_a', $msg);
        $this->assertThat(
            $this->info->getGroupTimeLag('group1', 'subject_a'),
            $this->logicalAnd(
                $this->greaterThanOrEqual(1000),
                $this->lessThanOrEqual(2000),
            ),
        );
        $this->assertGreaterThan(1000, $this->info->getGroupTimeLag('unknown', 'subject_a'));
        $this->assertSame(0, $this->info->getGroupTimeLag('group1', 'subject_unknown'));
    }

    public function testGetStreamLength(): void
    {
        $msg = ['k' => 'v'];
        $this->bus->add('subject_a', $msg);
        $this->assertSame(1, $this->info->getStreamLength('subject_a'));
        $this->bus->addMany('subject_a', [$msg, $msg, $msg, $msg]);
        $this->assertSame(5, $this->info->getStreamLength('subject_a'));
        $this->assertSame(0, $this->info->getStreamLength('subject_unknown'));
    }

    public function testXInfo(): void
    {
        $this->assertInstanceOf(StreamBusXInfoInterface::class, $this->info->xInfo('subject_a'));
    }
}
