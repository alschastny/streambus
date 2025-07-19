<?php

declare(strict_types=1);

namespace StreamBus\Parallel\Tasks;

use StreamBus\Parallel\ParallelTestCase;
use StreamBus\TestFactory;

class Consume
{
    public function __invoke(string $name, string $group, string $consumerName, int $iterations, int $count, int $blockMs, int $delay, string ...$subjects)
    {
        $builder = ParallelTestCase::createBuilder($name, $subjects);
        $consumer = $builder->createConsumer($group, $consumerName, $subjects);
        $logger = TestFactory::createLogger($consumerName);

        $logger->info('Start');
        $processed = 0;
        for ($i = 0; $i < $iterations; $i++) {
            $logger->debug('iteration ' . $i);
            $read = $consumer->read($count, $blockMs ?: null);
            foreach ($read as $subject => $messages) {
                $logger->debug('processing ' . $subject . ' read ' . count($messages) . ' items');
                $consumer->ack($subject, ...array_keys($messages));
                usleep($delay);
                $processed += count($messages);
            }
        }
        $logger->info('Processed ' . $processed);
        $logger->info('Finish');
    }
}
