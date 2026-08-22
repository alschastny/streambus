<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

final readonly class StreamBusSettings
{
    public const SCHEMA_VERSION = 1;

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

    public function toArray(): array
    {
        return [
            'min_ttl_ns' => $this->minTTLSec * 1_000_000_000,
            'max_size' => $this->maxSize,
            'exact_limits' => $this->exactLimits,
            'delete_on_ack' => $this->deleteOnAck,
            'delete_policy' => $this->deletePolicy->value,
            'max_delivery' => $this->maxDelivery,
            'ack_explicit' => $this->ackExplicit,
            'ack_wait_ns' => $this->ackWaitMs * 1_000_000,
            'nack_delay_ns' => $this->nackDelayMs * 1_000_000,
            'idmp_mode' => $this->idmpMode->value,
            'idmp_duration_sec' => $this->idmpDurationSec,
            'idmp_max_size' => $this->idmpMaxSize,
            'max_expired_subjects' => $this->maxExpiredSubjects,
        ];
    }

    public static function fromArray(array $data): self
    {
        return new self(
            minTTLSec: isset($data['min_ttl_ns']) ? (int) ($data['min_ttl_ns'] / 1_000_000_000) : 86400,
            maxSize: (int) ($data['max_size'] ?? 1000000),
            exactLimits: (bool) ($data['exact_limits'] ?? false),
            deleteOnAck: (bool) ($data['delete_on_ack'] ?? false),
            deletePolicy: DeleteMode::from((string) ($data['delete_policy'] ?? DeleteMode::KeepRef->value)),
            maxDelivery: (int) ($data['max_delivery'] ?? 0),
            ackExplicit: (bool) ($data['ack_explicit'] ?? true),
            ackWaitMs: isset($data['ack_wait_ns']) ? (int) ($data['ack_wait_ns'] / 1_000_000) : 30 * 60 * 1000,
            nackDelayMs: isset($data['nack_delay_ns']) ? (int) ($data['nack_delay_ns'] / 1_000_000) : 0,
            idmpMode: IdmpMode::from((string) ($data['idmp_mode'] ?? IdmpMode::None->value)),
            idmpDurationSec: (int) ($data['idmp_duration_sec'] ?? 0),
            idmpMaxSize: (int) ($data['idmp_max_size'] ?? 0),
            maxExpiredSubjects: (int) ($data['max_expired_subjects'] ?? 0),
        );
    }
}
