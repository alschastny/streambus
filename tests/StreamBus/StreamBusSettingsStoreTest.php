<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use StreamBus\TestFactory;

#[CoversClass(StreamBusSettingsStore::class)]
class StreamBusSettingsStoreTest extends TestCase
{
    private \Predis\Client $client;
    private StreamBusSettingsStore $store;
    private string $key;

    protected function setUp(): void
    {
        $this->client = TestFactory::createClient();
        $this->client->flushdb();
        $this->store = new StreamBusSettingsStore($this->client, 'test');
        $this->key = 'streambus:test:settings:v' . StreamBusSettings::SCHEMA_VERSION;
    }

    public function testLoadReturnsNullWhenNothingPublished(): void
    {
        $this->assertNull($this->store->load());
    }

    public function testSaveLoad(): void
    {
        $settings = new StreamBusSettings(
            minTTLSec: 120,
            maxSize: 500,
            exactLimits: true,
            deleteOnAck: true,
            deletePolicy: DeleteMode::DelRef,
            maxDelivery: 7,
            ackWaitMs: 90000,
            nackDelayMs: 1500,
            idmpMode: IdmpMode::Auto,
            idmpDurationSec: 3600,
            idmpMaxSize: 1000,
            maxExpiredSubjects: 4,
        );

        $this->assertTrue($this->store->save($settings));
        $this->assertEquals($settings, $this->store->load());
    }

    public function testLoadThrowsOnInvalidJson(): void
    {
        $this->client->set($this->key, '{not valid json');

        $this->expectException(StreamBusException::class);
        $this->expectExceptionMessage('failed to unserialize settings payload');
        $this->store->load();
    }

    public function testLoadThrowsOnNonArrayPayload(): void
    {
        $this->client->set($this->key, '123');

        $this->expectException(StreamBusException::class);
        $this->expectExceptionMessage('invalid settings payload at ' . $this->key);
        $this->store->load();
    }
}
