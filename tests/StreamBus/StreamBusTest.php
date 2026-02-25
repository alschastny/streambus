<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Predis\Client;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;

#[CoversClass(StreamBus::class)]
class StreamBusTest extends TestCase
{
    private Client $client;
    private StreamBus $bus;
    private StreamBusInfo $info;
    private StreamBus $busA;
    private StreamBus $busB;
    private StreamBusSettings $settings;

    protected function setUp(): void
    {
        $this->client = TestFactory::createClient();
        $this->client->flushdb();

        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 100,
            exactLimits: false,
            deleteOnAck: false,
            maxDelivery: 10,
            ackWaitMs: 30 * 60 * 1000,
        );

        $this->initWith('test', $settings);
    }

    protected function initWith(string $name, StreamBusSettings $settings): void
    {
        $this->settings = $settings;
        $builder = StreamBusBuilder::create($name)
            ->withClient($this->client)
            ->withSettings($this->settings)
            ->withSerializers([
                'subject_a' => new StreamBusJsonSerializer(),
                'subject_b' => new StreamBusJsonSerializer(),
                'subject_c' => new StreamBusJsonSerializer(),
                // Unprocessed types
                'subject_d' => new StreamBusJsonSerializer(),
                'subject_e' => new StreamBusJsonSerializer(),
                'subject_f' => new StreamBusJsonSerializer(),
            ]);

        $subjectsA = ['subject_a', 'subject_b', 'subject_c'];
        $subjectsB = ['subject_d', 'subject_e', 'subject_f'];

        $this->bus = $builder->createBus();
        $this->info = $builder->createBusInfo();
        $this->busA = $builder->withSubjects($subjectsA)->createBus();
        $this->busB = $builder->withSubjects($subjectsB)->createBus();

        $this->bus->createGroup('group1');
        $this->bus->createGroup('group2');
    }

    public function testAddWrongSubject(): void
    {
        $this->expectException(StreamBusException::class);
        $this->bus->add('unknown_subject', []);
    }

    public function testAddMany(): void
    {
        $messages = $this->makeMessages(100);
        $this->assertSame([], $this->bus->addMany('subject_a', []));
        $ids = $this->bus->addMany('subject_a', array_slice($messages, 0, 1));
        $this->assertCount(1, $ids);
        $ids = array_merge($ids, $this->bus->addMany('subject_a', array_slice($messages, 1, 1)));
        $this->assertCount(2, $ids);
        $ids = array_merge($ids, $this->bus->addMany('subject_a', array_slice($messages, 2)));
        $this->assertCount(100, $ids);

        $this->bus->createGroup('group1');
        $this->assertSame(
            ['subject_a' => array_combine($ids, $messages)],
            $this->bus->readNew('group1', 'consumer1', 100),
        );
    }

    public function testAddManyTTL(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 1,
            maxSize: 0,
            exactLimits: true,
        );
        $this->initWith(__FUNCTION__, $settings);

        $messages = $this->makeMessages(100);
        $ids = $this->bus->addMany('subject_a', $messages);
        $this->assertCount(100, $ids);
        $this->bus->createGroup('group1');
        $this->bus->createGroup('group2');
        $this->assertSame(
            ['subject_a' => array_combine($ids, $messages)],
            $this->bus->readNew('group1', 'consumer1', 100),
        );
        sleep(2);

        $messages2 = array_slice($messages, 0, 2);
        $ids = $this->bus->addMany('subject_a', $messages2);
        $this->assertCount(2, $ids);
        $this->assertSame(
            ['subject_a' => array_combine($ids, $messages2)],
            $this->bus->readNew('group2', 'consumer1', 100),
        );
    }

    public function testAddManyExactMaxSize(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 0,
            maxSize: 100,
            exactLimits: true,
        );
        $this->initWith(__FUNCTION__, $settings);

        $messages = $this->makeMessages(100);
        $ids = $this->bus->addMany('subject_a', array_slice($messages, 0, 1));
        $this->assertCount(1, $ids);
        $ids = $this->bus->addMany('subject_a', $messages);
        $this->assertCount(100, $ids);
        $this->assertSame(100, $this->info->getStreamLength('subject_a'));
        $ids = $this->bus->addMany('subject_a', $messages);
        $this->assertCount(100, $ids);
        $this->assertSame(100, $this->info->getStreamLength('subject_a'));
    }

    public function testAddRead(): void
    {
        $msgA = ['k1' => 'v1'];
        $msgB = ['k2' => 'v2'];
        $idA1 = $this->bus->add('subject_a', $msgA);
        $idA2 = $this->bus->add('subject_a', $msgB);
        $idB1 = $this->bus->add('subject_b', $msgA);
        $idB2 = $this->bus->add('subject_b', $msgB);

        $this->assertSame([
            'subject_a' => [$idA1 => $msgA, $idA2 => $msgB],
            'subject_b' => [$idB1 => $msgA, $idB2 => $msgB],
        ], $this->busA->readNew('group1', 'consumer1', 2));
        $this->assertSame([], $this->busA->readNew('group1', 'consumer1', 1));
        $this->assertSame([], $this->busA->readNew('group1', 'consumer2', 1));

        $this->assertSame([
            'subject_a' => [$idA1 => $msgA],
            'subject_b' => [$idB1 => $msgA],
        ], $this->busA->readNew('group2', 'consumer1', 1));
        $this->assertSame([
            'subject_a' => [$idA2 => $msgB],
            'subject_b' => [$idB2 => $msgB],
        ], $this->busA->readNew('group2', 'consumer2', 1));
        $this->assertSame([], $this->busA->readNew('group2', 'consumer1', 1));
        $this->assertSame([], $this->busA->readNew('group2', 'consumer2', 1));
    }

    public function testAddReadOnlyTypes(): void
    {
        $msg = ['k1' => 'v1'];
        $idA = $this->bus->add('subject_a', $msg);
        $idB = $this->bus->add('subject_b', $msg);
        $idC = $this->bus->add('subject_c', $msg);
        $idD = $this->bus->add('subject_d', $msg);
        $idE = $this->bus->add('subject_e', $msg);
        $idF = $this->bus->add('subject_f', $msg);

        $this->busA->createGroup('group1');
        $this->busB->createGroup('group2');

        $this->assertSame(
            [
                'subject_a' => [$idA => $msg],
                'subject_b' => [$idB => $msg],
                'subject_c' => [$idC => $msg],
            ],
            $this->busA->readNew('group1', 'consumer1', 1),
        );
        $this->assertSame([], $this->busA->readNew('group1', 'consumer2', 1));
        $this->assertSame(
            [
                'subject_d' => [$idD => $msg],
                'subject_e' => [$idE => $msg],
                'subject_f' => [$idF => $msg],
            ],
            $this->busB->readNew('group2', 'consumer1', 1),
        );
        $this->assertSame([], $this->busB->readNew('group2', 'consumer2', 1));
    }

    public function testAddReadWithBlock(): void
    {
        $msg = ['k1' => 'v1'];
        $idA = $this->bus->add('subject_a', $msg);
        $idB = $this->bus->add('subject_b', $msg);
        $this->assertSame(
            ['subject_a' => [$idA => $msg], 'subject_b' => [$idB => $msg]],
            $this->busA->readNew('group1', 'consumer1', 1, 10000),
        );
        $idC = $this->bus->add('subject_c', $msg);
        $time = microtime(true);
        $this->assertSame(
            ['subject_c' => [$idC => $msg]],
            $this->busA->readNew('group1', 'consumer1', 1, 10000),
        );
        $this->assertLessThan(1, microtime(true) - $time);
    }

    public function testAddAckRead(): void
    {
        $msg1 = ['k1' => 'v1'];
        $id1 = $this->bus->add('subject_a', $msg1);
        $msg2 = ['k2' => 'v2'];
        $id2 = $this->bus->add('subject_a', $msg2);

        $this->assertSame(
            ['subject_a' => [$id1 => $msg1]],
            $this->bus->readNew('group1', 'consumer1', 1),
        );
        $this->assertSame(
            ['subject_a' => [$id2 => $msg2]],
            $this->bus->readNew('group1', 'consumer2', 1),
        );
        $this->bus->ack('group1', 'subject_a', $id1, $id2);
        $this->assertSame([], $this->bus->readNew('group1', 'consumer1', 1));

        $this->assertSame(
            ['subject_a' => [$id1 => $msg1]],
            $this->bus->readNew('group2', 'consumer1', 1),
        );
        $this->assertSame(
            ['subject_a' => [$id2 => $msg2]],
            $this->bus->readNew('group2', 'consumer2', 1),
        );
        $this->bus->ack('group2', 'subject_a', $id1);
        $this->assertSame([], $this->bus->readNew('group2', 'consumer2', 1));

        $this->bus->nack('group2', 'consumer2', 'subject_a', $id2);
        $this->assertSame(
            ['subject_a' => [$id2 => $msg2]],
            $this->bus->readExpired('group2', 'consumer1', 1),
        );
    }

    public function testAddAckDeleteRead(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 1000,
            exactLimits: true,
            deleteOnAck: true,
            ackWaitMs: 30 * 60 * 1000,
        );
        $this->initWith(__FUNCTION__, $settings);

        $msg1 = ['k1' => 'v1'];
        $id1 = $this->bus->add('subject_a', $msg1);
        $msg2 = ['k2' => 'v2'];
        $id2 = $this->bus->add('subject_a', $msg2);
        $this->assertSame(
            ['subject_a' => [$id1 => $msg1]],
            $this->bus->readNew('group1', 'consumer1', 1),
        );
        $this->bus->ack('group1', 'subject_a', $id1);
        $this->assertSame(
            ['subject_a' => [$id2 => $msg2]],
            $this->bus->readNew('group2', 'consumer1', 1),
        );
        $this->bus->ack('group2', 'subject_a', $id2);
        $this->assertSame([], $this->bus->readNew('group1', 'consumer1', 1));
        $this->assertSame([], $this->bus->readNew('group2', 'consumer1', 1));
        $this->assertSame(0, $this->info->getGroupPending('group1', 'subject_a'));
        $this->assertSame(0, $this->info->getGroupPending('group2', 'subject_a'));
    }

    public function testDeletePolicyKeepRef(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 1000,
            exactLimits: true,
            deleteOnAck: true,
            deletePolicy: DeleteMode::KeepRef,
            ackWaitMs: 30 * 60 * 1000,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkDeleteModesSupport()) {
            $this->markTestSkipped('Redis >= 8.2 required for delete modes');
        }

        $msg = ['k1' => 'v1'];
        $id = $this->bus->add('subject_a', $msg);

        // Both groups read the message so both have it in their PEL
        $this->bus->readNew('group1', 'consumer1', 1);
        $this->bus->readNew('group2', 'consumer1', 1);

        // KeepRef: message is deleted from stream, group1 PEL ref removed,
        // but group2's PEL reference is preserved
        $this->assertSame(1, $this->bus->ack('group1', 'subject_a', $id));
        $this->assertSame(0, $this->info->getStreamLength('subject_a'));
        $this->assertSame(0, $this->info->getGroupPending('group1', 'subject_a'));
        $this->assertSame(1, $this->info->getGroupPending('group2', 'subject_a'));
        // group2 still has the entry in PEL but the data was deleted from stream
        $this->assertSame(
            ['subject_a' => [$id => null]],
            $this->bus->readPending('group2', 'consumer1', 1, null)[0],
        );
    }

    public function testDeletePolicyDelRef(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 1000,
            exactLimits: true,
            deleteOnAck: true,
            deletePolicy: DeleteMode::DelRef,
            ackWaitMs: 30 * 60 * 1000,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkDeleteModesSupport()) {
            $this->markTestSkipped('Redis >= 8.2 required for delete modes');
        }

        $msg = ['k1' => 'v1'];
        $id = $this->bus->add('subject_a', $msg);

        // Both groups read the message so both have it in their PEL
        $this->bus->readNew('group1', 'consumer1', 1);
        $this->bus->readNew('group2', 'consumer1', 1);

        // DelRef: message is deleted from stream AND all PEL references are removed,
        // including group2's PEL entry
        $this->assertSame(1, $this->bus->ack('group1', 'subject_a', $id));
        $this->assertSame(0, $this->info->getStreamLength('subject_a'));
        $this->assertSame(0, $this->info->getGroupPending('group1', 'subject_a'));
        $this->assertSame(0, $this->info->getGroupPending('group2', 'subject_a'));
    }

    public function testDeletePolicyAcked(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 1000,
            exactLimits: true,
            deleteOnAck: true,
            deletePolicy: DeleteMode::Acked,
            ackWaitMs: 30 * 60 * 1000,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkDeleteModesSupport()) {
            $this->markTestSkipped('Redis >= 8.2 required for delete modes');
        }

        $msg = ['k1' => 'v1'];
        $id = $this->bus->add('subject_a', $msg);

        // Both groups read the message so both have it in their PEL
        $this->bus->readNew('group1', 'consumer1', 1);
        $this->bus->readNew('group2', 'consumer1', 1);

        // Acked: group1 acks but message stays in stream because group2 hasn't acked yet
        $this->assertSame(1, $this->bus->ack('group1', 'subject_a', $id));
        $this->assertSame(1, $this->info->getStreamLength('subject_a'));
        $this->assertSame(0, $this->info->getGroupPending('group1', 'subject_a'));
        $this->assertSame(1, $this->info->getGroupPending('group2', 'subject_a'));

        // group2 acks — now all groups have acked, message is deleted from stream
        $this->assertSame(1, $this->bus->ack('group2', 'subject_a', $id));
        $this->assertSame(0, $this->info->getStreamLength('subject_a'));
        $this->assertSame(0, $this->info->getGroupPending('group2', 'subject_a'));
    }

    public function testAddNackRead(): void
    {
        $msg = ['k1' => 'v1'];
        $id = $this->bus->add('subject_a', $msg);
        $this->assertSame(
            ['subject_a' => [$id => $msg]],
            $this->bus->readNew('group1', 'consumer1', 1),
        );
        $this->assertSame(1, $this->bus->nack('group1', 'consumer1', 'subject_a', $id));
        $this->assertSame(
            ['subject_a' => [$id => $msg]],
            $this->bus->readExpired('group1', 'consumer1', 1),
        );
        $this->assertSame(1, $this->bus->nack('group1', 'consumer1', 'subject_a', $id));
        $this->assertSame(
            ['subject_a' => [$id => $msg]],
            $this->bus->readExpired('group1', 'consumer2', 1),
        );
        $this->assertSame([], $this->bus->readExpired('group1', 'consumer1', 1));
        $this->assertSame(
            ['subject_a' => [$id => $msg]],
            $this->bus->readNew('group2', 'consumer1', 1),
        );
        $this->assertSame([], $this->bus->readNew('group1', 'consumer2', 1));
        $this->assertSame(1, $this->bus->nack('group2', 'consumer1', 'subject_a', $id));
        $this->assertSame(
            ['subject_a' => [$id => $msg]],
            $this->bus->readExpired('group2', 'consumer2', 1),
        );
        $this->expectException(StreamBusException::class);
        $this->bus->nack('group1', 'consumer2', 'subject_a', $id, $this->settings->ackWaitMs + 1);
    }

    public function testAddNackReadOrder(): void
    {
        $msg1 = ['k1' => 'v1'];
        $id1 = $this->bus->add('subject_a', $msg1);
        $msg2 = ['k2' => 'v2'];
        $id2 = $this->bus->add('subject_a', $msg2);

        $this->assertSame(
            ['subject_a' => [$id1 => $msg1]],
            $this->bus->readNew('group1', 'consumer1', 1),
        );
        $this->assertSame(
            ['subject_a' => [$id2 => $msg2]],
            $this->bus->readNew('group1', 'consumer2', 1),
        );
        $this->assertSame(1, $this->bus->nack('group1', 'consumer2', 'subject_a', $id2));
        $this->assertSame(1, $this->bus->nack('group1', 'consumer1', 'subject_a', $id1));
        $this->assertSame(
            ['subject_a' => [$id1 => $msg1, $id2 => $msg2]],
            $this->bus->readExpired('group1', 'consumer1', 2),
        );
    }

    public function testAddNackDelayed(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 1000,
            exactLimits: false,
            deleteOnAck: false,
            maxDelivery: 10,
            ackWaitMs: 30 * 60 * 1000,
            nackDelayMs: 25,
        );
        $this->initWith(__FUNCTION__, $settings);

        $msg = ['k1' => 'v1'];
        $id = $this->bus->add('subject_a', $msg);
        $this->assertSame(
            ['subject_a' => [$id => $msg]],
            $this->bus->readNew('group1', 'consumer1', 1),
        );
        $this->assertSame(1, $this->bus->nack('group1', 'consumer1', 'subject_a', $id));
        $this->assertSame([], $this->bus->readNew('group1', 'consumer1', 1));
        $id1 = $this->bus->add('subject_a', $msg);
        $this->assertSame(
            ['subject_a' => [$id1 => $msg]],
            $this->bus->readNew('group1', 'consumer1', 1),
        );
        usleep($settings->nackDelayMs * 1000);
        $this->assertSame(1, $this->bus->nack('group1', 'consumer1', 'subject_a', $id1));
        $this->assertSame(
            ['subject_a' => [$id => $msg]],
            $this->bus->readExpired('group1', 'consumer1', 1),
        );
        $this->assertSame([], $this->bus->readExpired('group1', 'consumer1', 1));
        usleep($settings->nackDelayMs * 1000);
        $this->assertSame(
            ['subject_a' => [$id1 => $msg]],
            $this->bus->readExpired('group1', 'consumer1', 1),
        );
    }

    public function testNackMaxDelivery(): void
    {
        $msg1 = ['k1' => 'v1'];
        $id1 = $this->bus->add('subject_a', $msg1);
        $this->bus->createGroup('group1');

        for ($n = 1; $n <= $this->settings->maxDelivery; $n++) {
            $result = $this->bus->readNew('group1', 'consumer1', 1)
                ?: $this->bus->readExpired('group1', 'consumer1', 1);
            $this->assertSame(['subject_a' => [$id1 => $msg1]], $result);
            $this->bus->nack('group1', 'consumer1', 'subject_a', $id1);
        }
        $this->assertSame([], $this->bus->readNew('group1', 'consumer1', 1));
        $this->assertSame([], $this->bus->readExpired('group1', 'consumer1', 1));
    }

    public function testNackMaxDeliveryWithCallback(): void
    {
        $called = false;
        $msg1 = ['k1' => 'v1'];
        $id1 = $this->bus->add('subject_a', $msg1);
        $this->bus->setMaxAttemptsProcessor(function (string $id, array $msg) use ($id1, $msg1, &$called) {
            $called = true;
            $this->assertSame($msg, $msg1);
            $this->assertSame($id1, $id);
            return 1;
        });
        $this->bus->createGroup('group1');
        for ($n = 1; $n <= $this->settings->maxDelivery; $n++) {
            $result = $this->bus->readNew('group1', 'consumer1', 1)
                ?: $this->bus->readExpired('group1', 'consumer1', 1);
            $this->assertSame(['subject_a' => [$id1 => $msg1]], $result);
            $this->bus->nack('group1', 'consumer1', 'subject_a', $id1);
        }
        $this->assertTrue($called);
    }

    public function testNackDLQ(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 0,
            maxSize: 100,
            exactLimits: true,
            maxDelivery: 10,
        );

        $builder = StreamBusBuilder::create(__FUNCTION__)
            ->withClient($this->client)
            ->withSettings($settings)
            ->withSerializers([
                'subject_a' => new StreamBusJsonSerializer(),
            ]);

        $dlq = $builder->createDLQBus();
        $this->bus = $builder->withDLQ($dlq)->createBus();
        $dlq->createGroup('group1');
        $this->bus->createGroup('group1');
        $dlqInfo = $builder->createDLQBusInfo();

        $msg1 = ['k1' => 'v1'];
        $id1 = $this->bus->add('subject_a', $msg1);
        $msg2 = ['k2' => 'v2'];
        $this->bus->add('subject_a', $msg2);

        $this->assertSame(0, $dlqInfo->getStreamLength('subject_a'));
        for ($n = 1; $n <= 10; $n++) {
            $result = $this->bus->readExpired('group1', 'consumer1', 1)
                ?: $this->bus->readNew('group1', 'consumer1', 1);
            $this->assertSame(['subject_a' => [$id1 => $msg1]], $result);
            $this->bus->nack('group1', 'consumer1', 'subject_a', $id1);
        }

        $this->assertSame(1, $dlqInfo->getStreamLength('subject_a'));
        $result = $dlq->readNew('group1', 'consumer', 1);
        $this->assertSame($msg1, array_pop($result['subject_a']));
        $this->assertSame([], $dlq->readNew('group1', 'consumer', 1));
    }

    public function testNackDeletedOrUnknownId(): void
    {
        $this->bus->createGroup('group1');
        $this->assertSame(0, $this->bus->nack('group1', 'consumer1', 'subject_a', '1-1'));
    }

    public function testNoAck(): void
    {
        $settings = new StreamBusSettings(
            ackExplicit: false,
        );
        $this->initWith(__FUNCTION__, $settings);

        $msg1 = ['k1' => 'v1'];
        $id1 = $this->bus->add('subject_a', $msg1);
        $this->bus->readNew('group1', 'consumer1', 1);

        // Test item not in PEL list
        [$pending, $cursor] = $this->bus->readPending('group1', 'consumer1', 1, null);
        $this->assertSame([], $pending);

        // Test can't nack
        $this->expectException(StreamBusException::class);
        $this->bus->nack('group1', 'consumer1', 'subject_a', $id1);
    }

    public function testPendingRead(): void
    {
        $msg1 = ['k1' => 'v1'];
        $id1 = $this->bus->add('subject_a', $msg1);
        $msg2 = ['k2' => 'v2'];
        $id2 = $this->bus->add('subject_a', $msg2);
        $this->assertSame(
            ['subject_a' => [$id1 => $msg1]],
            $this->bus->readNew('group1', 'consumer1', 1),
        );
        $this->assertSame(
            ['subject_a' => [$id1 => $msg1]],
            $this->bus->readPending('group1', 'consumer1', 1, null)[0],
        );
        $this->bus->ack('group1', 'subject_a', $id1);
        $this->assertSame(
            [],
            $this->bus->readPending('group1', 'consumer1', 1, null)[0],
        );
        $this->assertSame(
            ['subject_a' => [$id2 => $msg2]],
            $this->bus->readNew('group1', 'consumer1', 1),
        );
        // Test with deleted element
        $msg3 = ['k3' => 'v3'];
        $id3 = $this->bus->add('subject_a', $msg3);
        $this->assertSame(
            ['subject_a' => [$id3 => $msg3]],
            $this->bus->readNew('group1', 'consumer1', 1),
        );
        $this->client->xdel('streambus:test:subject_a', $id2);
        $this->assertSame(
            ['subject_a' => [$id2 => null, $id3 => $msg3]],
            $this->bus->readPending('group1', 'consumer1', 2, null)[0],
        );
    }

    public function testPendingAfterAckWaitConsumeDelay(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 1000,
            exactLimits: false,
            deleteOnAck: false,
            maxDelivery: 10,
            ackWaitMs: 25,
        );
        $this->initWith(__FUNCTION__, $settings);

        $msg1 = ['k1' => 'v1'];
        $msg2 = ['k2' => 'v2'];
        $id1 = $this->bus->add('subject_a', $msg1);
        $id2 = $this->bus->add('subject_a', $msg2);
        $this->assertSame(
            ['subject_a' => [$id1 => $msg1]],
            $this->bus->readNew('group1', 'consumer1', 1),
        );
        $this->assertSame(
            ['subject_a' => [$id2 => $msg2]],
            $this->bus->readNew('group1', 'consumer2', 1),
        );
        usleep($settings->ackWaitMs * 1000);
        $this->assertSame(
            ['subject_a' => [$id1 => $msg1]],
            $this->bus->readPending('group1', 'consumer1', 1, null)[0],
        );
        $this->assertSame(
            ['subject_a' => [$id2 => $msg2]],
            $this->bus->readPending('group1', 'consumer2', 1, null)[0],
        );
        $this->bus->ack('group1', 'subject_a', $id2);
        $this->assertSame(
            [],
            $this->bus->readPending('group1', 'consumer2', 1, null)[0],
        );
        usleep($settings->ackWaitMs * 1000);
        $this->assertSame(
            ['subject_a' => [$id1 => $msg1]],
            $this->bus->readExpired('group1', 'consumer2', 1),
        );
    }

    public function testPendingCursorReads(): void
    {
        $messages = array_values($this->makeMessages(5));
        $ids = $this->bus->addMany('subject_a', $messages);
        $this->assertCount(5, $ids);
        [$id1, $id2, $id3, $id4, $id5] = $ids;
        [$msg1, $msg2, $msg3, $msg4, $msg5] = $messages;

        $this->bus->createGroup('group');

        $readItems = $this->bus->readNew('group', 'consumer1', 4);
        $this->assertSame(['subject_a' => [$id1 => $msg1, $id2 => $msg2, $id3 => $msg3, $id4 => $msg4]], $readItems);

        [$messages, $cursor1] = $this->bus->readPending('group', 'consumer1', 2, null);
        $this->assertSame(['subject_a' => [$id1 => $msg1, $id2 => $msg2]], $messages);

        [$messages, $cursor2] = $this->bus->readPending('group', 'consumer1', 2, $cursor1);
        $this->assertSame(['subject_a' => [$id3 => $msg3, $id4 => $msg4]], $messages);
        $this->assertNotEquals($cursor1, $cursor2);

        [$messages, $cursorLast] = $this->bus->readPending('group', 'consumer1', 1, $cursor2);
        $this->assertSame([], $messages);
        $this->assertSame($cursor2, $cursorLast);
    }

    public function testReadExpired(): void
    {
        $settings = new StreamBusSettings(
            maxExpiredSubjects: 3,
        );
        $this->initWith(__FUNCTION__, $settings);

        // Add elements, nack elements, read with count = 2
        $msg = ['k1', 'v1'];
        $idA = $this->bus->add('subject_a', $msg);
        $idB = $this->bus->add('subject_b', $msg);
        $idC = $this->bus->add('subject_c', $msg);
        $idD = $this->bus->add('subject_d', $msg);
        $idE = $this->bus->add('subject_e', $msg);
        $idF = $this->bus->add('subject_f', $msg);

        $this->bus->readNew('group1', 'consumer1', 1);
        $this->bus->nack('group1', 'consumer1', 'subject_a', $idA);
        $this->bus->nack('group1', 'consumer1', 'subject_b', $idB);
        $this->bus->nack('group1', 'consumer1', 'subject_c', $idC);
        $this->bus->nack('group1', 'consumer1', 'subject_d', $idD);
        $this->bus->nack('group1', 'consumer1', 'subject_e', $idE);
        $this->bus->nack('group1', 'consumer1', 'subject_f', $idF);

        $result = $this->bus->readExpired('group1', 'consumer1', 10);
        $this->assertCount(3, $result);
    }

    public function testReadExpiredDeleted(): void
    {
        $msg1 = ['k1', 'v1'];
        $id1 = $this->bus->add('subject_a', $msg1);

        $this->bus->createGroup('group1');
        $this->bus->readNew('group1', 'consumer1', 1);
        $this->bus->nack('group1', 'consumer1', 'subject_a', $id1);
        $this->client->xdel('streambus:test:subject_a', $id1);
        $result = $this->bus->readExpired('group1', 'consumer1', 10);
        $this->assertSame(['subject_a' => [$id1 => null]], $result);
    }

    public function testCreateGroup(): void
    {
        $this->assertTrue($this->bus->createGroup('test_group'));
        // Check already created group
        $this->assertTrue($this->bus->createGroup('test_group'));
        $this->expectException(StreamBusException::class);
        $this->bus->createGroup('');
    }

    public function testAddIdmpAutoDeduplicates(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 100,
            idmpMode: IdmpMode::Auto,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkIdmpSupport()) {
            $this->markTestSkipped('Redis >= 8.6 required for idempotency');
        }

        $msg = ['k' => 'v'];
        $id1 = $this->bus->add('subject_a', $msg, 'test-producer');
        $id2 = $this->bus->add('subject_a', $msg, 'test-producer');
        $this->assertSame($id1, $id2);
        $this->assertSame(1, $this->info->getStreamLength('subject_a'));
    }

    public function testAddIdmpAutoManyDeduplicates(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 100,
            idmpMode: IdmpMode::Auto,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkIdmpSupport()) {
            $this->markTestSkipped('Redis >= 8.6 required for idempotency');
        }

        $msgs = ['a' => ['k' => 'v1'], 'b' => ['k' => 'v2']];
        $ids1 = $this->bus->addMany('subject_a', $msgs, 'test-producer');
        $ids2 = $this->bus->addMany('subject_a', $msgs, 'test-producer');
        $this->assertSame($ids1, $ids2);
        $this->assertSame(2, $this->info->getStreamLength('subject_a'));
    }

    public function testAddIdmpExplicitDeduplicates(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 100,
            idmpMode: IdmpMode::Explicit,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkIdmpSupport()) {
            $this->markTestSkipped('Redis >= 8.6 required for idempotency');
        }

        $msg = ['k' => 'v'];
        $iid = 'unique-message-id';
        $id1 = $this->bus->add('subject_a', new StreamBusMessage($msg, null, $iid), 'test-producer');
        $id2 = $this->bus->add('subject_a', new StreamBusMessage($msg, null, $iid), 'test-producer');
        $this->assertSame($id1, $id2);
        $this->assertSame(1, $this->info->getStreamLength('subject_a'));
    }

    public function testAddIdmpExplicitDifferentIids(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 100,
            idmpMode: IdmpMode::Explicit,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkIdmpSupport()) {
            $this->markTestSkipped('Redis >= 8.6 required for idempotency');
        }

        $msg = ['k' => 'v'];
        $id1 = $this->bus->add('subject_a', new StreamBusMessage($msg, null, 'iid-1'), 'test-producer');
        $id2 = $this->bus->add('subject_a', new StreamBusMessage($msg, null, 'iid-2'), 'test-producer');
        $this->assertNotSame($id1, $id2);
        $this->assertSame(2, $this->info->getStreamLength('subject_a'));
    }

    public function testAddIdmpExplicitMissingIid(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 100,
            idmpMode: IdmpMode::Explicit,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkIdmpSupport()) {
            $this->markTestSkipped('Redis >= 8.6 required for idempotency');
        }

        $this->expectException(StreamBusException::class);
        $this->bus->add('subject_a', ['k' => 'v'], 'test-producer');
    }

    public function testAddIdmpExplicitManyDeduplicates(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 100,
            idmpMode: IdmpMode::Explicit,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkIdmpSupport()) {
            $this->markTestSkipped('Redis >= 8.6 required for idempotency');
        }

        $msgs = [
            new StreamBusMessage(['k' => 'v1'], null, 'iid-a'),
            new StreamBusMessage(['k' => 'v2'], null, 'iid-b'),
        ];
        $ids1 = $this->bus->addMany('subject_a', $msgs, 'test-producer');
        $ids2 = $this->bus->addMany('subject_a', $msgs, 'test-producer');
        $this->assertSame($ids1, $ids2);
        $this->assertSame(2, $this->info->getStreamLength('subject_a'));
    }

    public function testAddStreamBusMessageAutoDeduplicates(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 100,
            idmpMode: IdmpMode::Auto,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkIdmpSupport()) {
            $this->markTestSkipped('Redis >= 8.6 required for idempotency');
        }

        $msg = ['k' => 'v'];
        $id1 = $this->bus->add('subject_a', new StreamBusMessage($msg, null, ''), 'test-producer');
        $id2 = $this->bus->add('subject_a', new StreamBusMessage($msg, null, ''), 'test-producer');
        $this->assertSame($id1, $id2);
        $this->assertSame(1, $this->info->getStreamLength('subject_a'));
    }

    public function testAddStreamBusMessageExplicitDeduplicates(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 100,
            idmpMode: IdmpMode::Explicit,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkIdmpSupport()) {
            $this->markTestSkipped('Redis >= 8.6 required for idempotency');
        }

        $iid = 'msg-iid-1';
        $id1 = $this->bus->add('subject_a', new StreamBusMessage(['k' => 'v'], null, $iid), 'test-producer');
        $id2 = $this->bus->add('subject_a', new StreamBusMessage(['k' => 'v'], null, $iid), 'test-producer');
        $this->assertSame($id1, $id2);
        $this->assertSame(1, $this->info->getStreamLength('subject_a'));
    }

    public function testAddStreamBusMessageExplicitManyDeduplicates(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 60,
            maxSize: 100,
            idmpMode: IdmpMode::Explicit,
        );
        $this->initWith(__FUNCTION__, $settings);

        if (!$this->bus->checkIdmpSupport()) {
            $this->markTestSkipped('Redis >= 8.6 required for idempotency');
        }

        $items = [
            new StreamBusMessage(['k' => 'v1'], null, 'iid-a'),
            new StreamBusMessage(['k' => 'v2'], null, 'iid-b'),
        ];
        $ids1 = $this->bus->addMany('subject_a', $items, 'test-producer');
        $ids2 = $this->bus->addMany('subject_a', $items, 'test-producer');
        $this->assertSame($ids1, $ids2);
        $this->assertSame(2, $this->info->getStreamLength('subject_a'));
    }

    public function testAddStreamBusMessageCustomId(): void
    {
        $this->initWith(__FUNCTION__, $this->settings);

        $customId = '9999999999999-0';
        $returned = $this->bus->add('subject_a', new StreamBusMessage(['k' => 'v'], $customId, null));
        $this->assertSame($customId, $returned);
        $this->assertSame(1, $this->info->getStreamLength('subject_a'));
    }

    public function testAddManyStreamBusMessageCustomIds(): void
    {
        $this->initWith(__FUNCTION__, $this->settings);

        $items = [
            new StreamBusMessage(['k' => 'v1'], '9999999999997-0', null),
            new StreamBusMessage(['k' => 'v2'], '9999999999998-0', null),
            new StreamBusMessage(['k' => 'v3'], '9999999999999-0', null),
        ];
        $ids = $this->bus->addMany('subject_a', $items);
        $this->assertSame(['9999999999997-0', '9999999999998-0', '9999999999999-0'], $ids);
        $this->assertSame(3, $this->info->getStreamLength('subject_a'));
    }

    #[DataProvider('badSerializersProvider')]
    public function testBadSerializers(array $serializers): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new StreamBus('test', $this->client, $this->settings, $serializers);
    }

    public static function badSerializersProvider(): array
    {
        return [
            'empty' => [[]],
            'bad class' => [['subject_a' => new \stdClass()]],
            'bad subject' => [['subject~-=' => new StreamBusJsonSerializer()]],
        ];
    }

    private function makeMessages(int $count): array
    {
        $messages = [];
        for ($i = 1; $i <= $count; $i++) {
            $messages['key_' . $i] = ['k' . $i => 'v' . $i];
        }

        return $messages;
    }
}
