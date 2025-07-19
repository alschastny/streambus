<?php

declare(strict_types=1);

namespace StreamBus\Parallel;

use StreamBus\Parallel\Tasks\Consume;
use StreamBus\Parallel\Tasks\NackConsume;
use StreamBus\Parallel\Tasks\Produce;
use StreamBus\TestFactory;

class ProduceConsumeNackTest extends ParallelTestCase
{
    private const BUS_NAME = 'produce-consume-nack-test';
    private const SUBJECT = 'subject';

    public function test(): void
    {
        $client = TestFactory::createClient();
        $client->flushdb();

        $producer = $this->createProcess(Produce::class, [self::BUS_NAME, 50, 1, 0, self::SUBJECT]);

        $consumer1 = $this->createProcess(Consume::class, [self::BUS_NAME, 'group1', 'consumer1', 30, 1, 10, 1, self::SUBJECT]);
        $consumer2 = $this->createProcess(NackConsume::class, [self::BUS_NAME, 'group1', 'consumer2', 10, 1, self::SUBJECT]);
        $consumer3 = $this->createProcess(Consume::class, [self::BUS_NAME, 'group1', 'consumer3', 30, 1, 10, 1, self::SUBJECT]);

        $this->startAll($producer, $consumer1, $consumer2, $consumer3);
        $this->waitAll($producer, $consumer1, $consumer2, $consumer3);

        $info = ParallelTestCase::createBuilder(self::BUS_NAME, [self::SUBJECT])->createBusInfo();
        $this->assertSame(0, $info->getGroupPending('group1', self::SUBJECT));
        $this->assertSame([self::SUBJECT], $info->getSubjects());
        $this->assertSame([self::SUBJECT], $info->getSubjects());
    }
}
