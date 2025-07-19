<?php

declare(strict_types=1);

namespace StreamBus\Producer;

use StreamBus\StreamBus\StreamBusInterface;

final class StreamBusProducer implements StreamBusProducerInterface
{
    public function __construct(private readonly StreamBusInterface $bus) {}

    public function add(string $subject, mixed $item): string
    {
        return $this->bus->add($subject, $item);
    }

    public function addMany(string $subject, array $items): array
    {
        return $this->bus->addMany($subject, $items);
    }
}
