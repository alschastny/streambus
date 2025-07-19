<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

interface StreamBusInfoInterface
{
    public function getSubjects(): array;

    public function getGroups(string $subject): array;

    public function getGroupPending(string $group, string $subject): int;

    public function getGroupTimeLag(string $group, string $subject): int;

    public function getStreamLength(string $subject): int;

    public function xinfo(string $subject): StreamBusXInfoInterface;
}
