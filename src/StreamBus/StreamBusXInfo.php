<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

use Predis\Client;
use Predis\Command\Argument\Stream\XInfoStreamOptions;

final class StreamBusXInfo implements StreamBusXInfoInterface
{
    public function __construct(
        private readonly Client $client,
        private readonly string $streamKey,
    ) {}

    public function consumers(string $group): array
    {
        if (!$this->client->exists($this->streamKey)) {
            return [];
        }
        return $this->client->xinfo->consumers($this->streamKey, $group);
    }

    public function groups(): array
    {
        if (!$this->client->exists($this->streamKey)) {
            return [];
        }
        return $this->client->xinfo->groups($this->streamKey);
    }

    public function stream(bool $full = false, ?int $count = null): array
    {
        if (!$this->client->exists($this->streamKey)) {
            return [];
        }
        $options = $full ? (new XInfoStreamOptions())->full($count) : null;
        return $this->client->xinfo->stream($this->streamKey, $options);
    }
}
