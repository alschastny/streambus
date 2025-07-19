<?php

declare(strict_types=1);

namespace StreamBus\Consumer;

interface StreamBusConsumerInterface
{
    public function ack(string $subject, string ...$ids): int;

    public function nack(string $subject, string $id, ?int $nackDelayMs = null): int;

    /** @return array<string, array<string, mixed>> */
    public function read(int $count = 1, ?int $blockMs = null): array;
}
