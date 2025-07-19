<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

final class ResponseParser
{
    /** @return null|array<string, array<string, null|array> */
    public static function parseReadFormat(?array $data): ?array
    {
        if (!$data) {
            return $data;
        }

        if (is_string(array_key_first($data))) {
            return array_map(static fn($keyData) => self::parseEntries($keyData), $data);
        }

        $result = [];
        foreach ($data as [$key, $keyData]) {
            $result[$key] = self::parseEntries($keyData);
        }
        return $result;
    }

    public static function parseEntries($entries): array
    {
        $result = [];
        foreach ($entries as [$id, $dict]) {
            $result[$id] = $dict !== null ? self::parseDict($dict) : null;
        }
        return $result;
    }

    private static function parseDict(array $array): array
    {
        $result = [];
        foreach (array_chunk($array, 2) as [$key, $value]) {
            $result[$key] = $value;
        }
        return $result;
    }
}
