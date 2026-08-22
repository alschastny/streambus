<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

use Predis\Client;

final readonly class StreamBusSettingsStore
{
    private string $key;

    public function __construct(private Client $client, string $busName)
    {
        $this->key = 'streambus:' . $busName . ':settings:v' . StreamBusSettings::SCHEMA_VERSION;
    }

    public function save(StreamBusSettings $settings): bool
    {
        $result = $this->client->set($this->key, json_encode($settings->toArray(), JSON_THROW_ON_ERROR));

        return (string) $result === 'OK';
    }

    public function load(): ?StreamBusSettings
    {
        $json = $this->client->get($this->key);
        if ($json === null || $json === '') {
            return null;
        }

        try {
            $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new StreamBusException('failed to unserialize settings payload', 0, $e);
        }

        if (!is_array($data)) {
            throw new StreamBusException('invalid settings payload at ' . $this->key);
        }

        return StreamBusSettings::fromArray($data);
    }
}
