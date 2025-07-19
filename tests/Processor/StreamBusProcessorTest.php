<?php

declare(strict_types=1);

namespace StreamBus\Processor;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Predis\Client;
use Psr\Log\NullLogger;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBus\StreamBus;
use StreamBus\StreamBus\StreamBusSettings;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;

#[CoversClass(StreamBusProcessor::class)]
class StreamBusProcessorTest extends TestCase
{
    private Client $client;
    private StreamBusBuilder $builder;
    private StreamBus $bus;

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

        $this->builder = StreamBusBuilder::create('test')
            ->withClient($this->client)
            ->withSettings($settings)
            ->withSerializers([
                'subject_a' => new StreamBusJsonSerializer(),
                'subject_b' => new StreamBusJsonSerializer(),
                'subject_c' => new StreamBusJsonSerializer(),
                // Unprocessed types
                'subject_d' => new StreamBusJsonSerializer(),
                'subject_e' => new StreamBusJsonSerializer(),
                'subject_f' => new StreamBusJsonSerializer(),
            ]);

        $this->bus = $this->builder->createBus();
    }

    public function testProcess(): void
    {
        $msg1 = ['k1' => 'v1'];
        $this->bus->add('subject_a', $msg1);
        $this->bus->add('subject_b', $msg1);
        $this->bus->add('subject_c', $msg1);
        $this->bus->add('subject_d', $msg1);
        $this->bus->add('subject_e', $msg1);
        $this->bus->add('subject_f', $msg1);

        $processor = $this->builder->createProcessor(
            'group',
            'consumer',
            [
                'subject_a' => function (string $type, string $id, array $msg) use ($msg1) {
                    $this->assertSame($msg1, $msg);
                    return true;
                },
                'subject_b' => function (string $type, string $id, array $msg) use ($msg1) {
                    $this->assertSame($msg1, $msg);
                    return true;
                },
                'subject_c' => function (string $type, string $id, array $msg) use ($msg1) {
                    $this->assertSame($msg1, $msg);
                    return true;
                },
            ],
        );
        $processor
            ->setLogger(new NullLogger())
            ->setBlockMs(10)
            ->setReadBatch(1);

        $this->assertSame(3, $processor->process(5));
    }

    public function testFlakyProcess(): void
    {
        $msg1 = ['k1' => 'v1'];
        $this->bus->add('subject_a', $msg1);
        $this->bus->add('subject_a', $msg1);
        $this->bus->add('subject_b', $msg1);
        $this->bus->add('subject_b', $msg1);
        $this->bus->add('subject_c', $msg1);
        $this->bus->add('subject_c', $msg1);

        $processor = $this->builder->createProcessor(
            'group',
            'consumer',
            [
                'subject_a' => function (string $type, string $id, array $msg) {
                    static $tact = true;
                    return $tact = !$tact;
                },
                'subject_b' => function (string $type, string $id, array $msg) {
                    static $tact = true;
                    return $tact = !$tact;
                },
                'subject_c' => function (string $type, string $id, array $msg) {
                    static $tact = true;
                    return $tact = !$tact;
                },
            ],
        );
        $processor
            ->setBlockMs(10)
            ->setReadBatch(1);

        // Each message processed twice
        $this->assertSame(12, $processor->process(10));
    }

    public function testThrowWhileProcess(): void
    {
        $msg1 = ['k1' => 'v1'];
        $this->bus->add('subject_a', $msg1);

        $consumer = $this->builder->createConsumer('group', 'consumer', ['subject_a']);
        $processor = new StreamBusProcessor($consumer);
        $processor->setHandler('subject_a', function (string $type, string $id, array $msg) {
            return throw new \Exception('processing exception');
        });

        $this->expectException(\Exception::class);
        $processor->process(1);
    }

    public function testInterrupt(): void
    {
        $called = false;
        $interruptCallback = function () use (&$called) {
            return $called = true;
        };

        $consumer = $this->builder->createConsumer('group', 'consumer', ['subject_a']);
        $processor = (new StreamBusProcessor($consumer))
            ->setHandler('subject_a', fn(string $type, string $id, array $msg) => true)
            ->setInterruptCallback($interruptCallback);

        $this->assertSame(0, $processor->process(5));
        $this->assertTrue($called);
    }

    public function testEmptyHandler(): void
    {
        $msg1 = ['k1' => 'v1'];
        $this->bus->add('subject_a', $msg1);
        $idB = $this->bus->add('subject_b', $msg1);
        $this->bus->add('subject_c', $msg1);
        $this->bus->add('subject_d', $msg1);
        $this->bus->add('subject_e', $msg1);
        $this->bus->add('subject_f', $msg1);

        // Create deleted item in PEL
        $this->bus->createGroup('group');
        $this->bus->readNew('group', 'consumer', 1);
        $this->client->xdel('streambus:test:subject_b', $idB);

        $called = false;
        $emptyHandler = function () use (&$called) {
            return $called = true;
        };

        $processor = $this->builder->createProcessor(
            'group',
            'consumer',
            [
                'subject_a' => fn(string $type, string $id, array $msg) => true,
                'subject_b' => fn(string $type, string $id, array $msg) => true,
                'subject_c' => fn(string $type, string $id, array $msg) => true,
            ],
        );
        $processor
            ->setBlockMs(10)
            ->setEmptyHandler($emptyHandler);

        $this->assertSame(3, $processor->process(5));
        $this->assertTrue($called);
    }
}
