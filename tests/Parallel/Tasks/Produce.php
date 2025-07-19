<?php

declare(strict_types=1);

namespace StreamBus\Parallel\Tasks;

use StreamBus\Parallel\ParallelTestCase;
use StreamBus\TestFactory;

class Produce
{
    public function __invoke(string $name, int $count, int $batch, int $delay, string  ...$subjects)
    {
        $builder = ParallelTestCase::createBuilder($name, $subjects);
        $producer = $builder->createProducer();
        $logger = TestFactory::createLogger('producer');
        $logger->info('Start');
        $iterations = $count / $batch;
        $messagesBatch = array_fill(0, $batch, ['k' => 'v']);
        for ($i = 1; $i <= $iterations; $i++) {
            $subject = $subjects[array_rand($subjects)];
            $ids = $producer->addMany($subject, $messagesBatch);
            $logger->debug('ADDED TO ' . $subject . ' ids ' . implode(', ', $ids));
            usleep($delay);
        }
        $logger->info('Finish');
    }
}
