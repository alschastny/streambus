<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

final readonly class StreamBusMessage
{
    public function __construct(
        public mixed $item,
        public ?string $id = null,
        public ?string $idempotentId = null,
    ) {}
}
