<?php

namespace StreamBus\StreamBus;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

#[CoversClass(ResponseParser::class)]
class ResponseParserTest extends TestCase
{
    #[DataProvider('parseReadProvider')]
    public function testParseReadFormat(mixed $expected, mixed $data): void
    {
        $this->assertSame($expected, ResponseParser::parseReadFormat($data));
    }

    public static function parseReadProvider(): array
    {
        return [
            'null' => [null, null],
            'empty' => [[], []],
            'resp2' => [
                [
                    'stream1' => ['0-0' => ['k1' => 'v1']],
                    'stream2' => ['1-0' => null],
                    'stream3' => ['2-0' => ['k3' => 'v3']],
                ],
                [
                    ['stream1', [['0-0', ['k1', 'v1']]]],
                    ['stream2', [['1-0', null]]],
                    ['stream3', [['2-0', ['k3', 'v3']]]],
                ],
            ],
            'resp3' => [
                [
                    'stream1' => ['0-0' => ['k1' => 'v1']],
                    'stream2' => ['1-0' => null],
                    'stream3' => ['2-0' => ['k3' => 'v3']],
                ],
                [
                    'stream1' => [['0-0', ['k1', 'v1']]],
                    'stream2' => [['1-0', null]],
                    'stream3' => [['2-0', ['k3', 'v3']]],
                ],
            ],
        ];
    }

    #[DataProvider('parseDictProvider')]
    public function testParseEntries(mixed $expected, mixed $data): void
    {
        $this->assertSame($expected, ResponseParser::parseEntries($data));
    }

    public static function parseDictProvider(): array
    {
        return
            [
                'empty' => [[], []],
                'resp2 and resp3' => [
                    ['0-0' => ['k1' => 'v1', 'k2' => 'v2']],
                    [['0-0', ['k1', 'v1', 'k2', 'v2']]],
                ],
            ];
    }
}
