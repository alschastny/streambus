<?php

declare(strict_types=1);

namespace StreamBus\Producer;

interface StreamBusProducerInterface
{
    public function add(string $subject, mixed $item): string;

    public function addMany(string $subject, array $items): array;
}
