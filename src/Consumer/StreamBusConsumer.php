<?php

declare(strict_types=1);

namespace StreamBus\Consumer;

use StreamBus\StreamBus\StreamBusInterface;

// No order guaranties, at least once delivery
final class StreamBusConsumer implements StreamBusConsumerInterface
{
    private bool $readPending = true;
    private ?array $readPendingCursor = null;
    private bool $initialized = false;

    public function __construct(
        private readonly StreamBusInterface $bus,
        private readonly string $group,
        private readonly string $consumer,
    ) {}

    public function ack(string $subject, string ...$ids): int
    {
        $this->init();
        return $this->bus->ack($this->group, $subject, ...$ids);
    }

    public function nack(string $subject, string $id, ?int $nackDelayMs = null): int
    {
        $this->init();
        return $this->bus->nack($this->group, $this->consumer, $subject, $id, $nackDelayMs);
    }

    public function read(int $count = 1, ?int $blockMs = null): array
    {
        $this->init();
        if ($this->readPending) {
            [$messages, $this->readPendingCursor] = $this->bus->readPending($this->group, $this->consumer, $count, $this->readPendingCursor);
            if ($messages) {
                return $messages;
            }
            $this->readPending = false;
            $this->readPendingCursor = null;
        }

        return $this->bus->readExpired($this->group, $this->consumer, $count)
            ?: $this->bus->readNew($this->group, $this->consumer, $count, $blockMs);
    }

    private function init(): void
    {
        if ($this->initialized) {
            return;
        }
        $this->bus->createGroup($this->group) || throw new StreamBusConsumerException('can\'t create group ' . $this->group);
        $this->initialized = true;
    }
}
