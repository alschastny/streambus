<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

use Predis\Client;
use Predis\Response\ServerException;
use StreamBus\Serializer\StreamBusSerializerInterface;

final class StreamBus implements StreamBusInterface
{
    private string $streamKeysPrefix;
    private array $streamKeys = [];

    /** @var array<string, StreamBusSerializerInterface> */
    private array $serializers = [];

    private string $maxSizeOperator;
    private \Closure $addMethod;
    private bool $tact = false;
    private string $cursor = '0-0';

    private ?StreamBusInterface $deadLetterQueue = null;
    /** @var callable|null $maxAttemptsProcessor */
    private $maxAttemptsProcessor = null;

    private ?bool $supportsDeleteModes = null;  // Redis >= 8.2
    private ?bool $supportsIdempotency = null;  // Redis >= 8.6

    public function __construct(
        string $name,
        private Client $client,
        private StreamBusSettings $settings,
        array $serializers,
    ) {
        $this->streamKeysPrefix = 'streambus:' . $name . ':';
        $this->configureSerializers($serializers);
        $this->configureSettings();
    }

    private function configureSettings(): void
    {
        $this->maxSizeOperator = $this->settings->exactLimits ? '=' : '~';
        if ($this->settings->minTTLSec && $this->settings->maxSize) {
            $this->addMethod = fn($subject, $item, $producerId) => $this->addWithTact($subject, $item, $producerId);
        } elseif ($this->settings->minTTLSec) {
            $this->addMethod = fn($subject, $item, $producerId) => $this->addWithTTL($subject, $item, $producerId);
        } else {
            $this->addMethod = fn($subject, $item, $producerId) => $this->addWithSize($subject, $item, $producerId);
        }
    }

    private function configureSerializers(array $serializers): void
    {
        if (!$serializers) {
            throw new \InvalidArgumentException('no serializers provided');
        }
        foreach ($serializers as $subject => $serializer) {
            if (!$serializer instanceof StreamBusSerializerInterface) {
                throw new \InvalidArgumentException('each serializer must implement StreamBusSerializerInterface');
            }
            if (!preg_match('/^[\w.-]+$/', $subject)) {
                throw new \InvalidArgumentException('invalid serializer subject');
            }
            $this->serializers[$subject] = $serializer;
            $streamKey = $this->createStreamKey($subject);
            $this->streamKeys[] = $streamKey;
        }
    }

    public function setDeadLetterQueue(?StreamBusInterface $deadLetterQueue): self
    {
        $this->deadLetterQueue = $deadLetterQueue;

        return $this;
    }

    public function setMaxAttemptsProcessor(?callable $maxAttemptsProcessor): self
    {
        $this->maxAttemptsProcessor = $maxAttemptsProcessor;

        return $this;
    }

    private function checkSubject(string $subject): void
    {
        if (!array_key_exists($subject, $this->serializers)) {
            throw new StreamBusException('unknown message subject ' . $subject);
        }
    }

    private function createStreamKey(string $subject): string
    {
        return $this->streamKeysPrefix . $subject;
    }

    private function getSubjectFromStreamKey(string $key): string
    {
        return substr($key, strlen($this->streamKeysPrefix));
    }

    private function serialize(string $subject, mixed $item): array
    {
        return $this->serializers[$subject]->serialize($item);
    }

    private function unserialize(string $subject, array $item): mixed
    {
        return $this->serializers[$subject]->unserialize($item);
    }

    private function redisVersion(): string
    {
        $info = $this->client->info('server');
        return $info['Server']['redis_version'] ?? $info['redis_version'] ?? '0.0.0';
    }

    private static function compareVersion(string $a, string $b): int
    {
        $aParts = explode('.', $a);
        $bParts = explode('.', $b);
        $len = max(count($aParts), count($bParts));
        for ($i = 0; $i < $len; $i++) {
            $diff = (int) ($aParts[$i] ?? 0) <=> (int) ($bParts[$i] ?? 0);
            if ($diff !== 0) {
                return $diff;
            }
        }
        return 0;
    }

    public function checkDeleteModesSupport(): bool
    {
        return $this->supportsDeleteModes ??= self::compareVersion($this->redisVersion(), '8.2.0') >= 0;
    }

    public function checkIdmpSupport(): bool
    {
        return $this->supportsIdempotency ??= self::compareVersion($this->redisVersion(), '8.6.0') >= 0;
    }

    private function buildXaddOptions(array $trimArgs, ?string $idempotentId, string $producerId): array
    {
        $options = $trimArgs ? ['trim' => $trimArgs] : [];

        if ($this->checkDeleteModesSupport()) {
            $options['trimming'] = $this->settings->deletePolicy->value;
        }

        if ($this->settings->idmpMode === IdmpMode::None) {
            return $options;
        }

        if (!$this->checkIdmpSupport()) {
            throw new StreamBusException('Redis server does not support idempotency (requires 8.6+)');
        }

        if ($producerId === '') {
            throw new StreamBusException('producer Id is required in Explicit or Auto IDMP mode');
        }

        if ($this->settings->idmpMode === IdmpMode::Auto) {
            return $options + ['idmpauto' => $producerId];
        }

        return $options + [
            'idmp' => [
                $producerId,
                $idempotentId ?? throw new StreamBusException('idempotentId is required in Explicit IDMP mode'),
            ],
        ];
    }

    public function add(string $subject, mixed $item, string $producerId = ''): string
    {
        $this->checkSubject($subject);

        return ($this->addMethod)($subject, $item, $producerId);
    }

    public function addMany(string $subject, array $items, string $producerId = ''): array
    {
        $this->checkSubject($subject);

        if (!$items) {
            return [];
        }

        $lastKey = $penultimateKey = null;
        $penultimateTrimArgs = [];
        if ($this->settings->maxSize && $this->settings->minTTLSec) {
            if (count($items) >= 2) {
                /** @psalm-suppress PossiblyUndefinedArrayOffset */
                [$penultimateKey, $lastKey] = array_slice(array_keys($items), -2, 2);
                $lastTrimArgs = ['MINID', $this->maxSizeOperator, ($this->client->time()[0] - $this->settings->minTTLSec) * 1000];
                $penultimateTrimArgs = ['MAXLEN', $this->maxSizeOperator, $this->settings->maxSize];
            } else {
                $lastKey = array_key_last($items);
                $lastTrimArgs = ($this->tact = !$this->tact)
                    ? ['MINID', $this->maxSizeOperator, ($this->client->time()[0] - $this->settings->minTTLSec) * 1000]
                    : ['MAXLEN', $this->maxSizeOperator, $this->settings->maxSize];
            }
        } elseif ($this->settings->maxSize) {
            $lastTrimArgs = ['MAXLEN', $this->maxSizeOperator, $this->settings->maxSize];
        } else {
            $lastTrimArgs = ['MINID', $this->maxSizeOperator, ($this->client->time()[0] - $this->settings->minTTLSec) * 1000];
        }

        $ids = $this->client->pipeline(function ($pipe) use ($items, $lastKey, $penultimateKey, $penultimateTrimArgs, $lastTrimArgs, $subject, $producerId): void {
            $streamKey = $this->createStreamKey($subject);
            if ($lastKey === null) {
                $lastKey = array_key_last($items);
            }
            foreach ($items as $key => $item) {
                if ($item instanceof StreamBusMessage) {
                    $idempotentId = $item->idempotentId;
                    $entryId = $item->id ?? '*';
                    $item = $item->item;
                } else {
                    $entryId = '*';
                    $idempotentId = null;
                }
                $trimArgs = match ($key) {
                    $penultimateKey => $penultimateTrimArgs,
                    $lastKey => $lastTrimArgs,
                    default => [],
                };
                $pipe->xadd(
                    $streamKey,
                    $this->serialize($subject, $item),
                    $entryId,
                    $this->buildXaddOptions($trimArgs, $idempotentId, $producerId),
                );
            }
        });

        return array_map(static fn($id) => (string) $id, (array) $ids);
    }

    private function addWithTTL(string $subject, mixed $item, string $producerId): string
    {
        if ($item instanceof StreamBusMessage) {
            $idempotentId = $item->idempotentId;
            $entryId = $item->id ?? '*';
            $item = $item->item;
        } else {
            $entryId = '*';
            $idempotentId = null;
        }

        return (string) $this->client->xadd(
            $this->createStreamKey($subject),
            $this->serialize($subject, $item),
            $entryId,
            $this->buildXaddOptions(
                ['MINID', $this->maxSizeOperator, ($this->client->time()[0] - $this->settings->minTTLSec) * 1000],
                $idempotentId,
                $producerId,
            ),
        );
    }

    private function addWithSize(string $subject, mixed $item, string $producerId): string
    {
        if ($item instanceof StreamBusMessage) {
            $idempotentId = $item->idempotentId;
            $entryId = $item->id ?? '*';
            $item = $item->item;
        } else {
            $entryId = '*';
            $idempotentId = null;
        }

        return (string) $this->client->xadd(
            $this->createStreamKey($subject),
            $this->serialize($subject, $item),
            $entryId,
            $this->buildXaddOptions(
                ['MAXLEN', $this->maxSizeOperator, $this->settings->maxSize],
                $idempotentId,
                $producerId,
            ),
        );
    }

    private function addWithTact(string $subject, mixed $item, string $producerId): string
    {
        return ($this->tact = !$this->tact)
            ? $this->addWithTTL($subject, $item, $producerId)
            : $this->addWithSize($subject, $item, $producerId);
    }

    public function ack(string $group, string $subject, string ...$ids): int
    {
        !$this->settings->ackExplicit && throw new StreamBusException('no ack mode enabled');
        $this->checkSubject($subject);

        $key = $this->createStreamKey($subject);

        if ($this->settings->deleteOnAck && $this->checkDeleteModesSupport()) {
            return count(array_filter(
                $this->client->xackdel($key, $group, $this->settings->deletePolicy->value, $ids),
                fn(int $v) => $v > 0,
            ));
        }

        $result = $this->client->xack($key, $group, ...$ids);

        if ($this->settings->deleteOnAck) {
            $this->client->xdel($key, ...$ids);
        }

        return $result;
    }

    public function nack(string $group, string $consumer, string $subject, string $id, ?int $nackDelayMs = null): int
    {
        !$this->settings->ackExplicit && throw new StreamBusException('no ack mode enabled');

        if ($nackDelayMs > $this->settings->nackDelayMs) {
            throw new StreamBusException('nackDelay > nackWaitMs');
        }

        $this->checkSubject($subject);
        $key = $this->createStreamKey($subject);

        // before xclaim we need to check message still belongs to consumer
        if (
            (!$pendingInfo = $this->client->xpending($key, $group, null, $id, $id, 1, $consumer))
            || (!$item = $pendingInfo[0] ?? null)
        ) {
            return 0;
        }

        [$id, /* $consumer */, $lastDeliveredMs, $deliveredCount] = $item;

        if ($this->settings->maxDelivery && $deliveredCount >= $this->settings->maxDelivery) {
            $item = null;
            if ($this->maxAttemptsProcessor || $this->deadLetterQueue) {
                if ($itemData = $this->client->xrange($key, $id, $id, 1)) {
                    $item = $this->unserialize($subject, array_pop($itemData));
                }
            }

            if (!$this->ack($group, $subject, $id)) {
                return 0; // @codeCoverageIgnore
            }

            if (!$this->deadLetterQueue && !$this->maxAttemptsProcessor) {
                return 1;
            }

            if ($this->deadLetterQueue && $item) {
                $this->deadLetterQueue->add($subject, $item);
            }

            return $this->maxAttemptsProcessor ? (int) ($this->maxAttemptsProcessor)($id, $item) : 1;
        }

        $newIdleTime = max(0, max($lastDeliveredMs, $this->settings->ackWaitMs) - ($nackDelayMs ?? $this->settings->nackDelayMs));
        $ids = $this->client->xclaim($key, $group, '__NACK__', $lastDeliveredMs, $id, $newIdleTime, null, null, false, true);

        return (int) (isset($ids[0]) && $ids[0] === $id);
    }

    public function readNew(string $group, string $consumer, int $count, ?int $blockMs = null): array
    {
        $itemsMap = ResponseParser::parseReadFormat(
            $this->client->xreadgroup(
                $group,
                $consumer,
                $count,
                $blockMs,
                !$this->settings->ackExplicit,
                ...$this->streamKeys,
                ...array_fill(0, count($this->streamKeys), '>'),
            ),
        ) ?? [];

        $result = [];
        foreach ($itemsMap as $key => $items) {
            /** @var array<string, array> $items */
            $subject = $this->getSubjectFromStreamKey($key);
            $result[$subject] = array_map(
                fn($item) => $this->unserialize($subject, $item),
                $items,
            );
        }

        return $result;
    }

    public function readExpired(string $group, string $consumer, int $count): array
    {
        $keys = $this->streamKeys;
        if ($this->settings->maxExpiredSubjects && count($keys) > $this->settings->maxExpiredSubjects) {
            shuffle($keys);
            $keys = array_slice($keys, 0, $this->settings->maxExpiredSubjects);
        }

        $result = [];
        foreach ($keys as $key) {
            [$this->cursor, $claimed, $deleted] = $this->client->xautoclaim($key, $group, $consumer, $this->settings->ackWaitMs, $this->cursor, $count, false);
            $subject = $this->getSubjectFromStreamKey($key);
            $result[$subject] = [];
            foreach ($deleted as $id) {
                $result[$subject][$id] = null;
            }
            $claimedParsed = ResponseParser::parseEntries($claimed);
            $result[$subject] += array_map(fn($item) => $this->unserialize($subject, $item), $claimedParsed);
            ksort($result[$subject]);
            if (0 >= $count -= count($result[$subject])) {
                break;
            }
        }

        return array_filter($result);
    }

    public function readPending(string $group, string $consumer, int $count, ?array $cursor): array
    {
        $cursor = $cursor ?? array_fill_keys($this->streamKeys, '0');
        $result = [];
        $itemsMap = ResponseParser::parseReadFormat(
            $this->client->xreadgroup(
                $group,
                $consumer,
                $count,
                null,
                !$this->settings->ackExplicit,
                ...$this->streamKeys,
                ...array_values($cursor),
            ),
        ) ?? [];

        foreach ($itemsMap as $key => $items) {
            if (!$items) {
                continue;
            }
            $subject = $this->getSubjectFromStreamKey($key);
            $result[$subject] = array_map(
                fn($item) => $item !== null ? $this->unserialize($subject, $item) : null,
                $items,
            );
            $cursor[$key] = array_key_last($items);
        }

        return [$result, $cursor];
    }

    public function createGroup(string $group, string $id = '0'): bool
    {
        if ($group === '') {
            throw new StreamBusException('group name can\'t be empty');
        }

        foreach ($this->streamKeys as $key) {
            if ($this->client->exists($key)) {
                $keyGroups = array_column($this->client->xinfo->groups($key), 'name');
                if (in_array($group, $keyGroups, true)) {
                    continue;
                }
            }

            try {
                if ((string) $this->client->xgroup->create($key, $group, $id, true) !== 'OK') {
                    return false;
                }
                // @codeCoverageIgnoreStart
            } catch (ServerException) {
                // Check because of concurrent calls
                $keyGroups = array_column($this->client->xinfo->groups($key), 'name');
                if (in_array($group, $keyGroups, true)) {
                    continue;
                }
                return false;
                // @codeCoverageIgnoreEnd
            }
            $this->applyIdmpConfig($key);
        }

        return true;
    }

    private function applyIdmpConfig(string $streamKey): void
    {
        if ($this->settings->idmpMode === IdmpMode::None || !$this->checkIdmpSupport()) {
            return;
        }

        $duration = $this->settings->idmpDurationSec > 0 ? $this->settings->idmpDurationSec : null;
        $maxSize = $this->settings->idmpMaxSize > 0 ? $this->settings->idmpMaxSize : null;

        if ($duration !== null || $maxSize !== null) {
            $this->client->xcfgset($streamKey, $duration, $maxSize);
        }
    }
}
