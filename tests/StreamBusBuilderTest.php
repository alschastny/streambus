<?php

namespace StreamBus;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use StreamBus\Consumer\StreamBusConsumer;
use StreamBus\Consumer\StreamBusOrderedConsumer;
use StreamBus\Consumer\StreamBusOrderedStrictConsumer;
use StreamBus\Processor\StreamBusProcessor;
use StreamBus\Producer\StreamBusProducer;
use StreamBus\Serializer\StreamBusJsonSerializer;
use StreamBus\StreamBus\StreamBus;
use StreamBus\StreamBus\StreamBusInfo;
use StreamBus\StreamBus\StreamBusSettings;

#[CoversClass(StreamBusBuilder::class)]
class StreamBusBuilderTest extends TestCase
{
    public function testBuilder(): void
    {
        $builder = StreamBusBuilder::create('test')
            ->withClient(TestFactory::createClient())
            ->withSettings(new StreamBusSettings())
            ->withSerializers([
                'subject_a' => new StreamBusJsonSerializer(),
                'subject_b' => new StreamBusJsonSerializer(),
                'subject_c' => new StreamBusJsonSerializer(),
            ]);
        $dlq = $builder->createBus();

        $builder = $builder
            ->withMaxAttemptsProcessor(static fn() => true)
            ->withDLQ($dlq);

        $this->assertInstanceOf(StreamBus::class, $builder->createBus());
        $this->assertInstanceOf(StreamBus::class, $builder->createDLQBus());
        $this->assertInstanceOf(StreamBusInfo::class, $builder->createBusInfo());
        $this->assertInstanceOf(StreamBusInfo::class, $builder->createDLQBusInfo());

        $this->assertInstanceOf(StreamBusProducer::class, $builder->createProducer('producer'));
        $this->assertInstanceOf(StreamBusConsumer::class, $builder->createConsumer('group', 'consumer'));
        $this->assertInstanceOf(StreamBusProcessor::class, $builder->createProcessor(
            'group',
            'test',
            [
                'subject_a' => fn(string $type, string $id, mixed $msg) => true,
                'subject_c' => fn(string $type, string $id, mixed $msg) => true,
            ],
        ));
        $this->assertInstanceOf(StreamBusOrderedConsumer::class, $builder->createOrderedConsumer('group', ['subject_a', 'subject_b']));
        $this->assertInstanceOf(StreamBusProcessor::class, $builder->createOrderedProcessor(
            'group',
            [
                'subject_a' => fn(string $type, string $id, mixed $msg) => true,
                'subject_c' => fn(string $type, string $id, mixed $msg) => true,
            ],
        ));
        $this->assertInstanceOf(StreamBusOrderedStrictConsumer::class, $builder->createOrderedStrictConsumer('group', ['subject_a', 'subject_b']));
        $this->assertInstanceOf(StreamBusProcessor::class, $builder->createOrderedStrictProcessor(
            'group',
            [
                'subject_a' => fn(string $type, string $id, mixed $msg) => true,
                'subject_c' => fn(string $type, string $id, mixed $msg) => true,
            ],
        ));
        $this->expectException(\InvalidArgumentException::class);
        $this->assertInstanceOf(StreamBusProcessor::class, $builder->createOrderedStrictProcessor(
            'group',
            [
                'subject_a' => fn(string $type, string $id, mixed $msg) => true,
                'subject_unknown' => fn(string $type, string $id, mixed $msg) => true,
            ],
        ));
    }
}
