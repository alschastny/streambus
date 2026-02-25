<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

interface StreamBusInterface
{
    public function add(string $subject, mixed $item, string $producerId = ''): string;

    public function addMany(string $subject, array $items, string $producerId = ''): array;

    public function ack(string $group, string $subject, string ...$ids): int;

    public function nack(string $group, string $consumer, string $subject, string $id, ?int $nackDelayMs = null): int;

    /** @return array<string, array<string, mixed>> */
    public function readNew(string $group, string $consumer, int $count, ?int $blockMs = null): array;

    /** @return array<string, array<string, mixed>> */
    public function readExpired(string $group, string $consumer, int $count): array;

    /** @return array{array<string, array<string, mixed>>, array} */
    public function readPending(string $group, string $consumer, int $count, ?array $cursor): array;

    public function createGroup(string $group, string $id = '0'): bool;
}
