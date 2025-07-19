<?php

declare(strict_types=1);

namespace StreamBus\Consumer;

use StreamBus\StreamBus\StreamBusInfoInterface;
use StreamBus\StreamBus\StreamBusInterface;

// Ordered, at least once delivery, throws if inconsistency detected
final class StreamBusOrderedStrictConsumer implements StreamBusConsumerInterface
{
    private bool $readPending = true;
    private ?array $readPendingCursor = null;
    private bool $initialized = false;
    private array $pending = [];

    public function __construct(
        private readonly StreamBusInterface $bus,
        private readonly StreamBusInfoInterface $info,
        private readonly string $group,
        private readonly string $consumer,
        private readonly array $subjects,
    ) {}

    public function ack(string $subject, string ...$ids): int
    {
        $this->init();
        $acked = $this->bus->ack($this->group, $subject, ...$ids);
        if ($acked !== count($ids)) {
            throw new StreamBusConsumerException('not acked');
        }
        $this->pending[$subject] -= $acked;
        $this->checkPending($subject);

        return $acked;
    }

    public function nack(string $subject, string $id, ?int $nackDelayMs = null): int
    {
        throw new StreamBusConsumerException('not allowed');
    }

    public function read(int $count = 1, ?int $blockMs = null): array
    {
        $this->init();
        if ($this->readPending) {
            [$items, $this->readPendingCursor] = $this->bus->readPending($this->group, $this->consumer, $count, $this->readPendingCursor);
            if ($items) {
                return $items;
            }
            $this->readPending = false;
            $this->readPendingCursor = null;
        }
        $items = $this->bus->readNew($this->group, $this->consumer, $count, $blockMs);
        foreach ($items as $subject => $subjectItems) {
            $this->pending[$subject] += count($subjectItems);
        }
        return $items;
    }

    private function init(): void
    {
        if ($this->initialized) {
            return;
        }
        $this->bus->createGroup($this->group) || throw new StreamBusConsumerException('can\'t create group ' . $this->group);

        foreach ($this->subjects as $subject) {
            $this->pending[$subject] = 0;
            if ($consumers = $this->info->xinfo($subject)->consumers($this->group)) {
                if (count($consumers) !== 1) {
                    throw new StreamBusConsumerException('only one consumer allowed, detected ' . count($consumers));
                }
                $consumer = array_pop($consumers);
                if ($consumer['name'] !== $this->consumer) {
                    throw new StreamBusConsumerException('unknown consumer detected ' . $consumer['name']);
                }
                $this->pending[$subject] = $consumer['pending'] ?? 0;
            }
        }

        $this->initialized = true;
    }

    private function checkPending(string $subject): void
    {
        $consumers = $this->info->xinfo($subject)->consumers($this->group);
        if (count($consumers) !== 1) {
            throw new StreamBusConsumerException('only one consumer allowed, detected ' . count($consumers));
        }
        $consumer = array_pop($consumers);
        if ($this->pending[$subject] !== $consumer['pending']) {
            throw new StreamBusConsumerException('inconsistency detected, calculated pending ' . $this->pending[$subject] . ' not eq real pending ' . $consumer['pending']);
        }
    }
}
