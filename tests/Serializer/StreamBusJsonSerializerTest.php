<?php

declare(strict_types=1);

namespace StreamBus\Serializer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(StreamBusJsonSerializer::class)]
class StreamBusJsonSerializerTest extends TestCase
{
    public function test(): void
    {
        $serializer = new StreamBusJsonSerializer(
            static fn(TestMessage $msg) => $msg->toArray(),
            static fn(array $data) => TestMessage::fromArray($data),
        );

        $msg = new TestMessage('1');
        $data = $serializer->serialize($msg);
        $deserialized = $serializer->unserialize($data);
        $this->assertEquals($msg, $deserialized);
    }

    public function testSerializeException(): void
    {
        $serializer = new StreamBusJsonSerializer(
            static fn(TestMessage $msg) => $msg->toArray(),
        );
        $this->expectException(StreamBusSerializerException::class);
        $serializer->serialize('');
    }

    public function testUnserializeException(): void
    {
        $serializer = new StreamBusJsonSerializer();
        $this->expectException(StreamBusSerializerException::class);
        $serializer->unserialize(['json' => '{']);
    }
}
