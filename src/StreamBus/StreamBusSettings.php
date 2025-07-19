<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

final readonly class StreamBusSettings
{
    public function __construct(
        // Retention policy
        public int $minTTLSec = 86400,
        public int $maxSize = 1000000,
        public bool $exactLimits = false,
        public bool $deleteOnAck = false,

        // Delivery policy
        public int $maxDelivery = 0,

        // Ack policy
        public bool $ackExplicit = true,
        public int $ackWaitMs = 30 * 60 * 1000,
        public int $nackDelayMs = 0,

        // Other
        public int $maxExpiredSubjects = 0,
    ) {
        if ($minTTLSec < 0) {
            throw new \InvalidArgumentException('negative minSecTTL');
        }

        if ($maxSize < 0) {
            throw new \InvalidArgumentException('negative maxSize');
        }

        if (!$minTTLSec && !$maxSize) {
            throw new \InvalidArgumentException('minSecTTL and maxSize eq 0');
        }

        if ($ackWaitMs < 0) {
            throw new \InvalidArgumentException('negative ackWaitMs');
        }

        if ($nackDelayMs < 0) {
            throw new \InvalidArgumentException('negative nackDelay');
        }

        if ($this->nackDelayMs > $this->ackWaitMs) {
            throw new \InvalidArgumentException('nackDelay > ackWaitMs');
        }

        if ($this->maxExpiredSubjects < 0) {
            throw new \InvalidArgumentException('negative maxExpiredSubjects');
        }

        if ($this->deleteOnAck && !$this->ackExplicit) {
            throw new \InvalidArgumentException('deleteOnAck and ackExplicit can\'t be used together');
        }
    }
}
