<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

interface StreamBusXInfoInterface
{
    public function consumers(string $group): array;

    public function groups(): array;

    public function stream(bool $full = false, ?int $count = null): array;
}
