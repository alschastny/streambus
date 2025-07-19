<?php

declare(strict_types=1);

namespace StreamBus\Benchmark;

use Monolog\Level;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBus\StreamBus;
use StreamBus\StreamBus\StreamBusSettings;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;

class SteamBusBenchmarkTest extends TestCase
{
    private StreamBus $bus;
    private StreamBusBuilder $busBuilder;
    private StreamBusSettings $settings;
    private LoggerInterface $logger;

    private int $limit = 10000;

    protected function setUp(): void
    {
        $client = TestFactory::createClient();
        $client->flushdb();

        $this->logger = TestFactory::createLogger('benchmark', Level::Debug);

        $this->settings = new StreamBusSettings(
            minTTLSec: 86400,
            maxSize: 1000000,
            exactLimits: false,
            deleteOnAck: false,
            maxDelivery: 10,
            ackWaitMs: 30 * 60 * 1000,
        );

        $builder = StreamBusBuilder::create('test')
            ->withClient($client)
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

        $this->busBuilder = $builder;
        $this->bus = $builder->createBus();
    }

    public function testAdd(): void
    {
        $limit = 50_000;
        $msg = ['k' => 'v'];
        $time = microtime(true);
        for ($i = 1; $i <= $limit; $i++) {
            $this->bus->add('subject_a', $msg);
        }
        $time = microtime(true) - $time;
        $this->logger->info('add', ['mps' => floor($limit / $time), 'length' => $limit, 'time' => $time]);
        $this->assertTrue(true);
    }

    public function testAddMulti(): void
    {
        $limit = 1_000_000;
        $msg = ['k' => 'v'];
        $messages = array_fill(0, $limit, $msg);
        $time = microtime(true);
        foreach (array_chunk($messages, 1000) as $chunk) {
            $this->bus->addMany('subject_a', $chunk);
        }
        $time = microtime(true) - $time;
        $this->logger->info('addMulti', ['mps' => floor($limit / $time), 'length' => $limit, 'time' => $time]);
        $this->assertTrue(true);
    }

    #[DataProvider('batchProvider')]
    public function testSingleHandling(int $batch): void
    {
        $msg = ['k' => 'v'];
        for ($i = 1; $i <= $this->limit; $i++) {
            $this->bus->add('subject_a', $msg);
        }

        $processor = $this->busBuilder->createProcessor('group', 'test', [
            'subject_a' => fn(string $type, string $id, mixed $msg) => true,
        ])->setReadBatch($batch);

        $time = microtime(true);
        $processed = $processor->process($this->limit);
        $time = microtime(true) - $time;
        $this->logger->info('handling 1 type', ['mps' => floor($processed / $time), 'processed' => $processed, 'length' => $this->limit, 'batch' => $batch, 'time' => $time]);
        $this->assertTrue(true);
    }

    #[DataProvider('batchProvider')]
    public function testMultipleHandlersHandling(int $batch): void
    {
        $msg = ['k' => 'v'];
        for ($i = 1; $i <= $this->limit; $i++) {
            $this->bus->add('subject_a', $msg);
            $this->bus->add('subject_b', $msg);
            $this->bus->add('subject_c', $msg);
        }

        $processor = $this->busBuilder->createProcessor('group', 'test', [
            'subject_a' => fn(string $type, string $id, mixed $msg) => true,
            'subject_b' => fn(string $type, string $id, mixed $msg) => true,
            'subject_c' => fn(string $type, string $id, mixed $msg) => true,
        ])->setReadBatch($batch);


        $time = microtime(true);
        $processed = $processor->process((int) ($this->limit / $batch));
        $time = microtime(true) - $time;
        $this->logger->info('handling 3 types', ['mps' => floor($processed / $time), 'processed' => $processed, 'length' => $this->limit, 'batch' => $batch, 'time' => $time]);
        $this->assertTrue(true);
    }

    #[DataProvider('batchProvider')]
    public function testMultipleHandlersOrderedHandling(int $batch): void
    {
        $msg = ['k' => 'v'];
        for ($i = 1; $i <= $this->limit; $i++) {
            $this->bus->add('subject_a', $msg);
            $this->bus->add('subject_b', $msg);
            $this->bus->add('subject_c', $msg);
        }

        $consumer = $this->busBuilder->createOrderedProcessor('group', [
            'subject_a' => fn(string $type, string $id, mixed $msg) => true,
            'subject_b' => fn(string $type, string $id, mixed $msg) => true,
            'subject_c' => fn(string $type, string $id, mixed $msg) => true,
        ])->setReadBatch($batch);

        $time = microtime(true);
        $processed = $consumer->process((int) ($this->limit / $batch));
        $time = microtime(true) - $time;
        $this->logger->info('ordered handling 3 types', ['mps' => floor($processed / $time), 'processed' => $processed, 'length' => $this->limit, 'batch' => $batch, 'time' => $time]);
        $this->assertTrue(true);
    }

    public static function batchProvider(): array
    {
        return [[1], [10], [100], [1000]];
    }
}
