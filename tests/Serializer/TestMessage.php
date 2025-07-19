<?php

declare(strict_types=1);

namespace StreamBus\Serializer;

class TestMessage
{
    public function __construct(private string $value) {}

    public function toArray(): array
    {
        return ['value' => $this->value];
    }

    public static function fromArray(array $data): self
    {
        return new self((string) $data['value']);
    }
}
