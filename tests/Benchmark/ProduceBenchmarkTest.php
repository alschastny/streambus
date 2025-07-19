<?php

declare(strict_types=1);

namespace StreamBus\Benchmark;

use StreamBus\Parallel\ParallelTestCase;
use StreamBus\Parallel\Tasks\Produce;
use StreamBus\TestFactory;

class ProduceBenchmarkTest extends ParallelTestCase
{
    private const BUS_NAME = 'produce-benchmark';
    private const SUBJECTS = ['subject_a'];

    public function test(): void
    {
        $client = TestFactory::createClient();
        $client->flushdb();

        $messagesNum = 5_000_000;
        $producersNum = 5;
        $producersBatchNum = 5_000;
        $producerMessagesNum = $messagesNum / $producersNum;

        $producers = [];
        for ($i = 1; $i <= $producersNum; $i++) {
            $producers[] = $this->createProcess(Produce::class, [self::BUS_NAME, $producerMessagesNum, $producersBatchNum, 0, ...self::SUBJECTS]);
        }

        $time = microtime(true);
        $this->startAll(...$producers);
        $this->waitAll(...$producers);

        TestFactory::createLogger(self::BUS_NAME)->info(
            'Produce {messages} messages took {sec} sec, {mps} messages per second',
            ['messages' => $messagesNum, 'sec' => $sec = microtime(true) - $time, 'mps' => $messagesNum / $sec],
        );

        $info = ParallelTestCase::createBuilder(self::BUS_NAME, self::SUBJECTS)->createBusInfo();
        $this->assertSame($messagesNum, $info->getStreamLength(self::SUBJECTS[0]));
        $this->assertSame(self::SUBJECTS, $info->getSubjects());
    }
}
