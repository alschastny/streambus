<?php

declare(strict_types=1);

namespace StreamBus\Parallel;

use PHPUnit\Framework\TestCase;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBus\StreamBusSettings;
use StreamBus\StreamBusBuilder;
use StreamBus\TestFactory;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\PhpExecutableFinder;
use Symfony\Component\Process\Process;

class ParallelTestCase extends TestCase
{
    protected function setUp(): void
    {
        $logger = TestFactory::createLogger(static::class);
        $logger->info('Start');
        parent::setUp();
    }

    protected function tearDown(): void
    {
        $logger = TestFactory::createLogger(static::class);
        $logger->info('Finish');
        parent::tearDown();
    }

    public static function createBuilder(string $name, array $subjects): StreamBusBuilder
    {
        $client = TestFactory::createClient();
        $settings = new StreamBusSettings(
            minTTLSec: 86400,
            maxSize: 10_000_000,
            exactLimits: false,
            deleteOnAck: false,
            maxDelivery: 10,
            ackWaitMs: 1000,
        );

        return StreamBusBuilder::create($name)
            ->withClient($client)
            ->withSettings($settings)
            ->withSerializers(array_fill_keys($subjects, new StreamBusJsonSerializer()));
    }

    protected function createProcess(string $class, array $arguments): Process
    {
        $process = new Process([(new PhpExecutableFinder())->find(), __DIR__ . '/Run.php', $class, ...$arguments]);
        $process->setTty(true);
        return $process;
    }

    protected function waitAll(Process ...$processes): void
    {
        foreach ($processes as $process) {
            $process->wait();
            if ($process->isSuccessful()) {
                continue;
            }
            throw new ProcessFailedException($process);
        }
    }

    protected function startAll(Process ...$processes): void
    {
        foreach ($processes as $process) {
            $process->start();
        }
    }
}
