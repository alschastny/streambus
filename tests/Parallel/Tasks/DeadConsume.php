<?php

declare(strict_types=1);

namespace StreamBus\Parallel\Tasks;

use StreamBus\Parallel\ParallelTestCase;
use StreamBus\TestFactory;

class DeadConsume
{
    public function __invoke(string $name, string $group, string $consumerName, int $count, int $delay, string ...$subjects)
    {
        $builder = ParallelTestCase::createBuilder($name, $subjects);
        $consumer = $builder->createConsumer($group, $consumerName, $subjects);
        $logger = TestFactory::createLogger($consumerName);

        $logger->info('Start');
        $processed = 0;
        for ($i = 0; $i < $count; $i++) {
            $logger->debug('iteration ' . $i);
            $read = $consumer->read(1, 10);
            foreach ($read as $subject => $messages) {
                $logger->debug('processing ' . $subject . ' read ' . count($messages) . ' items');
                foreach ($messages as $key => $message) {
                    $logger->debug('DEAD ' . $key);
                    usleep($delay);
                    $processed++;
                }
            }
        }
        $logger->info('Processed ' . $processed);
        $logger->info('Finish');
    }
}
