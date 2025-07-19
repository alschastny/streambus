<?php

declare(strict_types=1);

namespace StreamBus\Serializer;

interface StreamBusSerializerInterface
{
    /** @return array<string, string|int|float|bool> */
    public function serialize($item): array;

    /** @param array<string, string|int|float|bool> $data */
    public function unserialize(array $data): mixed;
}
