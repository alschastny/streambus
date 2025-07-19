<?php

declare(strict_types=1);

namespace StreamBus\Parallel;

use StreamBus\Parallel\Tasks\Consume;
use StreamBus\Parallel\Tasks\DeadConsume;
use StreamBus\Parallel\Tasks\Produce;
use StreamBus\TestFactory;

class ProduceConsumeDeadTest extends ParallelTestCase
{
    private const BUS_NAME = 'produce-consume-test';
    private const SUBJECT = 'subject';

    public function test(): void
    {
        $client = TestFactory::createClient();
        $client->flushdb();

        $producer = $this->createProcess(Produce::class, [self::BUS_NAME, 50, 1, 0, self::SUBJECT]);

        $consumer1 = $this->createProcess(Consume::class, [self::BUS_NAME, 'group1', 'consumer1', 30, 1, 10, 100_000, self::SUBJECT]);
        $consumer2 = $this->createProcess(DeadConsume::class, [self::BUS_NAME, 'group1', 'consumer2_dead', 5, 100_000, self::SUBJECT]);
        $consumer3 = $this->createProcess(Consume::class, [self::BUS_NAME, 'group1', 'consumer2', 30, 1, 10, 100_000, self::SUBJECT]);

        $this->startAll($producer, $consumer1, $consumer2, $consumer3);
        $this->waitAll($producer, $consumer1, $consumer2, $consumer3);

        $info = ParallelTestCase::createBuilder(self::BUS_NAME, [self::SUBJECT])->createBusInfo();
        $this->assertSame(0, $info->getGroupPending('group1', self::SUBJECT));
        $this->assertSame([self::SUBJECT], $info->getSubjects());
    }
}
