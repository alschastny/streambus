<?php

declare(strict_types=1);

namespace StreamBus;

use Monolog\Handler\StreamHandler;
use Monolog\Level;
use Monolog\Processor\PsrLogMessageProcessor;
use Predis\Client;
use Psr\Log\LoggerInterface;

class TestFactory
{
    public static function createLogger(string $name, Level $level = Level::Info): LoggerInterface
    {
        $logger = new \Monolog\Logger($name);
        $logger->pushHandler(new StreamHandler('php://stdout', $level));
        $logger->pushProcessor(new PsrLogMessageProcessor());
        return $logger;
    }

    public static function createClient(): Client
    {
        $redisHost = getenv('REDIS_HOST') ?: '127.0.0.1';
        $redisPort = (int) getenv('REDIS_PORT') ?: 6379;
        $redisRESPVersion = (int) (getenv('REDIS_RESP_VERSION') ?: 2);
        return new Client(
            [
                'host' => $redisHost,
                'port' => $redisPort,
                'database' => 15,
                'protocol' => $redisRESPVersion,
            ],
        );
    }
}
