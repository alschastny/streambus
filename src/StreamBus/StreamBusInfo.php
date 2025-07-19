<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

use Predis\Client;

final class StreamBusInfo implements StreamBusInfoInterface
{
    private string $streamKeysPrefix;

    public function __construct(
        private Client $client,
        string $name,
    ) {
        $this->streamKeysPrefix = 'streambus:' . $name . ':';
    }

    private function createStreamKey(string $subject): string
    {
        return $this->streamKeysPrefix . $subject;
    }

    public function getSubjects(): array
    {
        $subjectKeys = $this->client->keys($this->streamKeysPrefix . '*');
        $subjects = array_map(fn(string $subjectKey): string => substr($subjectKey, strlen($this->streamKeysPrefix)), $subjectKeys);
        sort($subjects);
        return $subjects;
    }

    public function getGroupPending(string $group, string $subject): int
    {
        $key = $this->createStreamKey($subject);
        if (!$this->client->exists($key)) {
            return 0;
        }
        foreach ($this->client->xinfo->groups($key) as $info) {
            if ($info['name'] === $group) {
                return $info['pending'];
            }
        }
        return 0;
    }

    public function getGroupTimeLag(string $group, string $subject): int
    {
        $key = $this->createStreamKey($subject);
        if (!$this->client->exists($key)) {
            return 0;
        }

        /** @psalm-suppress UndefinedMagicMethod */
        $lastGeneratedId = $this->client->xinfo->stream($key)['last-generated-id'] ?? '0-0';
        $lastDeliveredId = '0-0';
        foreach ($this->client->xinfo->groups($key) as $groupInfo) {
            if ($groupInfo['name'] !== $group) {
                continue;
            }
            $lastDeliveredId = $groupInfo['last-delivered-id'];
        }
        $lastGenerated = (int) explode('-', $lastGeneratedId, 2)[0];
        $lastDelivered = (int) explode('-', $lastDeliveredId, 2)[0];

        return $lastDelivered > $lastGenerated ? 0 : ($lastGenerated - $lastDelivered);
    }

    public function getGroups(string $subject): array
    {
        $key = $this->createStreamKey($subject);
        if (!$this->client->exists($key)) {
            return [];
        }
        return array_column($this->client->xinfo->groups($this->createStreamKey($subject)), 'name');
    }

    public function getStreamLength(string $subject): int
    {
        $key = $this->createStreamKey($subject);
        if (!$this->client->exists($key)) {
            return 0;
        }
        return $this->client->xlen($key);
    }

    public function xinfo(string $subject): StreamBusXInfoInterface
    {
        return new StreamBusXInfo($this->client, $this->createStreamKey($subject));
    }
}
