<?php

declare(strict_types=1);

namespace StreamBus\Parallel;

use StreamBus\Parallel\Tasks\Consume;
use StreamBus\Parallel\Tasks\Produce;
use StreamBus\TestFactory;

class ProduceConsumeTest extends ParallelTestCase
{
    private const BUS_NAME = 'produce-consume-test';
    private const SUBJECTS = ['subject_a', 'subject_b', 'subject_c'];

    public function test(): void
    {
        $client = TestFactory::createClient();
        $client->flushdb();

        $producer = $this->createProcess(Produce::class, [self::BUS_NAME, 50, 1, 0, ...self::SUBJECTS]);

        $consumer1 = $this->createProcess(Consume::class, [self::BUS_NAME, 'group1', 'consumer1', 25, 1, 10, 1, ...self::SUBJECTS]);
        $consumer2 = $this->createProcess(Consume::class, [self::BUS_NAME, 'group1', 'consumer2', 25, 1, 10, 1, ...self::SUBJECTS]);
        $consumer3 = $this->createProcess(Consume::class, [self::BUS_NAME, 'group1', 'consumer3', 25, 1, 10, 1, ...self::SUBJECTS]);
        $consumer4 = $this->createProcess(Consume::class, [self::BUS_NAME, 'group2', 'consumer1', 25, 1, 10, 1, ...self::SUBJECTS]);
        $consumer5 = $this->createProcess(Consume::class, [self::BUS_NAME, 'group2', 'consumer2', 25, 1, 10, 1, ...self::SUBJECTS]);
        $consumer6 = $this->createProcess(Consume::class, [self::BUS_NAME, 'group2', 'consumer3', 25, 1, 10, 1, ...self::SUBJECTS]);

        $this->startAll($producer, $consumer1, $consumer2, $consumer3, $consumer4, $consumer5, $consumer6);
        $this->waitAll($producer, $consumer1, $consumer2, $consumer3, $consumer4, $consumer5, $consumer6);

        $info = ParallelTestCase::createBuilder(self::BUS_NAME, self::SUBJECTS)->createBusInfo();
        foreach (self::SUBJECTS as $subject) {
            $this->assertSame(0, $info->getGroupPending('group1', $subject));
            $this->assertSame(0, $info->getGroupPending('group2', $subject));
        }
        $this->assertSame(self::SUBJECTS, $info->getSubjects());
    }
}
