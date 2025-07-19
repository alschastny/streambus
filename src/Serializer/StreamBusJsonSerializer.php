<?php

declare(strict_types=1);

namespace StreamBus\Serializer;

final class StreamBusJsonSerializer implements StreamBusSerializerInterface
{
    /** @var callable|null $toArray */
    private $toArray;
    /** @var callable|null $fromArray */
    private $fromArray;

    public function __construct(
        ?callable $toArray = null,
        ?callable $fromArray = null,
    ) {
        $this->toArray = $toArray;
        $this->fromArray = $fromArray;
    }

    public function serialize($item): array
    {
        try {
            $data = $this->toArray ? ($this->toArray)($item) : $item;
            return ['json' => json_encode($data, JSON_THROW_ON_ERROR)];
        } catch (\Throwable $e) {
            throw new StreamBusSerializerException('failed to serialize data', 0, $e);
        }
    }

    public function unserialize(array $data): mixed
    {
        try {
            $data = json_decode((string) ($data['json'] ?? ''), true, 512, JSON_THROW_ON_ERROR);
            return $this->fromArray ? ($this->fromArray)($data) : $data;
        } catch (\JsonException $e) {
            throw new StreamBusSerializerException('failed to unserialize data', 0, $e);
        }
    }
}
