<?php

declare(strict_types=1);

namespace StreamBus\Benchmark;

use StreamBus\Parallel\ParallelTestCase;
use StreamBus\Parallel\Tasks\Consume;
use StreamBus\Parallel\Tasks\Produce;
use StreamBus\TestFactory;

class ConsumeBenchmarkTest extends ParallelTestCase
{
    private const BUS_NAME = 'produce-consume-benchmark';
    private const SUBJECTS = ['subject_a'];

    public function test(): void
    {
        $client = TestFactory::createClient();
        $client->flushdb();

        $messagesNum = 1_000_000;
        $consumersNum = 6;
        $consumeBatchNum = 1_000;
        $iterations = $messagesNum / $consumersNum / $consumeBatchNum;

        $producer = $this->createProcess(Produce::class, [self::BUS_NAME, $messagesNum, $consumeBatchNum, 0, ...self::SUBJECTS]);
        $this->startAll($producer);
        $this->waitAll($producer);

        $consumers = [];
        for ($i = 1; $i <= $consumersNum; $i++) {
            $consumers[] = $this->createProcess(Consume::class, [self::BUS_NAME, 'group1', 'consumer' . $i, $iterations + 1, $consumeBatchNum, 0, 0, ...self::SUBJECTS]);
        }

        $time = microtime(true);
        $this->startAll(...$consumers);
        $this->waitAll(...$consumers);

        TestFactory::createLogger(self::BUS_NAME)->info(
            'Consume {messages} messages took {sec} sec, {mps} messages per second',
            ['messages' => $messagesNum, 'sec' => $sec = microtime(true) - $time, 'mps' => $messagesNum / $sec],
        );

        $info = ParallelTestCase::createBuilder(self::BUS_NAME, self::SUBJECTS)->createBusInfo();
        foreach (self::SUBJECTS as $subject) {
            $this->assertSame(0, $info->getGroupPending('group1', $subject));
        }
        $this->assertSame(self::SUBJECTS, $info->getSubjects());
    }
}
