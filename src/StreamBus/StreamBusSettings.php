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
        // Delete mode (Redis 8.2+)
        public DeleteMode $deletePolicy = DeleteMode::KeepRef,

        // Delivery policy
        public int $maxDelivery = 0,

        // Ack policy
        public bool $ackExplicit = true,
        public int $ackWaitMs = 30 * 60 * 1000,
        public int $nackDelayMs = 0,


        // Idempotency policy (Redis 8.6+)
        public IdmpMode $idmpMode = IdmpMode::None,
        public int $idmpDurationSec = 0,      // 0 = server default (XCFGSET IDMP-DURATION)
        public int $idmpMaxSize = 0,          // 0 = server default (XCFGSET IDMP-MAXSIZE)

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

        if ($this->idmpDurationSec < 0) {
            throw new \InvalidArgumentException('negative idmpDurationSec');
        }

        if ($this->idmpMaxSize < 0) {
            throw new \InvalidArgumentException('negative idmpMaxSize');
        }
    }
}
