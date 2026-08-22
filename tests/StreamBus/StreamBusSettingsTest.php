<?php

namespace StreamBus\StreamBus;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

#[CoversClass(StreamBusSettings::class)]
class StreamBusSettingsTest extends TestCase
{
    #[DataProvider('exceptionProvider')]
    public function testBadParameters(array $params): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new StreamBusSettings(...$params);
    }

    public static function exceptionProvider(): array
    {
        return [
            'negative minTTLSec' => [['minTTLSec' => -1]],
            'negative maxSize' => [['maxSize' => -1]],
            'minTTLSec and minTTLSec are zeroes' => [['minTTLSec' => 0, 'maxSize' => 0]],
            'negative ackWaitMs' => [['ackWaitMs' => -1]],
            'negative nackDelayMs' => [['nackDelayMs' => -1]],
            'nackDelayMs greater than ackWaitMs' => [['nackDelayMs' => 10000, 'ackWaitMs' => 5000]],
            'negative maxExpiredSubjects' => [['maxExpiredSubjects' => -1]],
            'both ackExplicit and deleteOnAck' => [['ackExplicit' => false, 'deleteOnAck' => true]],
            'negative idmpDurationSec' => [['idmpDurationSec' => -1]],
            'negative idmpMaxSize' => [['idmpMaxSize' => -1]],
        ];
    }

    public function testArrayRoundTrip(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 120,
            maxSize: 500,
            exactLimits: true,
            deleteOnAck: true,
            deletePolicy: DeleteMode::Acked,
            maxDelivery: 7,
            ackExplicit: true,
            ackWaitMs: 90000,
            nackDelayMs: 1500,
            idmpMode: IdmpMode::Explicit,
            idmpDurationSec: 3600,
            idmpMaxSize: 1000,
            maxExpiredSubjects: 4,
        );

        $restored = StreamBusSettings::fromArray($settings->toArray());

        $this->assertEquals($settings, $restored);
    }

    public function testFromArrayUsesDefaultsForMissingFields(): void
    {
        $this->assertEquals(new StreamBusSettings(), StreamBusSettings::fromArray([]));
    }
}
