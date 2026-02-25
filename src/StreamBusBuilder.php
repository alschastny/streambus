<?php

declare(strict_types=1);

namespace StreamBus;

use Predis\Client;
use StreamBus\Consumer\StreamBusConsumer;
use StreamBus\Consumer\StreamBusOrderedConsumer;
use StreamBus\Consumer\StreamBusOrderedStrictConsumer;
use StreamBus\Processor\StreamBusProcessor;
use StreamBus\Producer\StreamBusProducer;
use StreamBus\StreamBus\StreamBus;
use StreamBus\StreamBus\StreamBusInfo;
use StreamBus\StreamBus\StreamBusInterface;
use StreamBus\StreamBus\StreamBusSettings;

class StreamBusBuilder
{
    protected ?StreamBusSettings $settings = null;
    protected ?Client $client = null;
    protected array $serializers = [];
    protected array $subjects = [];

    protected ?StreamBusInterface $dlq = null;
    /** @var callable|null $interruptCallback */
    protected $maxAttemptsProcessor = null;

    protected function __construct(
        protected readonly string $name,
    ) {}

    public static function create(string $name): static
    {
        return new static($name);
    }

    public function withSettings(StreamBusSettings $settings): static
    {
        $builder = clone $this;
        $builder->settings = $settings;
        return $builder;
    }

    public function withClient(Client $client): static
    {
        $builder = clone $this;
        $builder->client = $client;
        return $builder;
    }

    public function withSerializers(array $serializers): static
    {
        $builder = clone $this;
        $builder->serializers = $serializers;
        return $builder;
    }

    public function withDLQ(StreamBusInterface $dlq): static
    {
        $builder = clone $this;
        $builder->dlq = $dlq;
        return $builder;
    }

    public function withMaxAttemptsProcessor(callable $maxAttemptsProcessor): static
    {
        $builder = clone $this;
        $builder->maxAttemptsProcessor = $maxAttemptsProcessor;
        return $builder;
    }

    public function withSubjects(array $subjects): static
    {
        $builder = clone $this;
        $builder->subjects = $subjects;
        return $builder;
    }

    public function createBus(): StreamBus
    {
        return (new StreamBus(
            $this->name,
            $this->client ?? throw new \InvalidArgumentException('client is not defined'),
            $this->settings ?? throw new \InvalidArgumentException('settings is not defined'),
            $this->getBusSerializers() ?: throw new \InvalidArgumentException('serializers are empty'),
        ))
            ->setDeadLetterQueue($this->dlq)
            ->setMaxAttemptsProcessor($this->maxAttemptsProcessor);
    }

    public function createDLQBus(): StreamBus
    {
        return (new StreamBus(
            'dlq:' . $this->name,
            $this->client ?? throw new \InvalidArgumentException('client is not defined'),
            $this->settings ?? throw new \InvalidArgumentException('settings is not defined'),
            $this->getBusSerializers() ?: throw new \InvalidArgumentException('serializers are empty'),
        ));
    }

    protected function getBusSerializers(): array
    {
        if (!$this->subjects) {
            return $this->serializers;
        }

        if ($notFound = array_diff_key(array_flip($this->subjects), $this->serializers)) {
            throw new \InvalidArgumentException('serializers are not defined for the following subjects: ' . implode(', ', array_keys($notFound)));
        }

        return array_intersect_key($this->serializers, array_flip($this->subjects));
    }

    public function createBusInfo(): StreamBusInfo
    {
        $client = $this->client ?? throw new \InvalidArgumentException('client is not defined');
        return new StreamBusInfo($client, $this->name);
    }

    public function createDLQBusInfo(): StreamBusInfo
    {
        $client = $this->client ?? throw new \InvalidArgumentException('client is not defined');
        return new StreamBusInfo($client, 'dlq:' . $this->name);
    }

    public function createProducer(string $name): StreamBusProducer
    {
        return new StreamBusProducer($name, $this->createBus());
    }

    public function createConsumer(string $group, string $consumer, array $subjects = []): StreamBusConsumer
    {
        $bus = $subjects ? $this->withSubjects($subjects)->createBus() : $this->createBus();
        return new StreamBusConsumer($bus, $group, $consumer);
    }

    public function createProcessor(string $group, string $consumer, array $handlers): StreamBusProcessor
    {
        $busConsumer = $this->createConsumer($group, $consumer, array_keys($handlers));
        $settings = $this->settings ?? throw new \InvalidArgumentException('settings is not defined');
        return (new StreamBusProcessor($busConsumer))
            ->setHandlers($handlers)
            ->setAckMode($settings->ackExplicit, $settings->ackExplicit);
    }

    public function createOrderedConsumer(string $group, array $subjects = []): StreamBusOrderedConsumer
    {
        $bus = $subjects ? $this->withSubjects($subjects)->createBus() : $this->createBus();
        return new StreamBusOrderedConsumer($bus, $group, 'consumer');
    }

    public function createOrderedProcessor(string $group, array $handlers): StreamBusProcessor
    {
        $busConsumer = $this->createOrderedConsumer($group, array_keys($handlers));
        $settings = $this->settings ?? throw new \InvalidArgumentException('settings is not defined');
        return (new StreamBusProcessor($busConsumer))
            ->setHandlers($handlers)
            ->setAckMode($settings->ackExplicit, false);
    }

    public function createOrderedStrictConsumer(string $group, array $subjects = []): StreamBusOrderedStrictConsumer
    {
        $bus = $subjects ? $this->withSubjects($subjects)->createBus() : $this->createBus();
        return new StreamBusOrderedStrictConsumer($bus, $this->createBusInfo(), $group, 'consumer', $subjects ?: $this->subjects);
    }

    public function createOrderedStrictProcessor(string $group, array $handlers): StreamBusProcessor
    {
        $busConsumer = $this->createOrderedStrictConsumer($group, array_keys($handlers));
        $settings = $this->settings ?? throw new \InvalidArgumentException('settings is not defined');
        return (new StreamBusProcessor($busConsumer))
            ->setHandlers($handlers)
            ->setAckMode($settings->ackExplicit, false);
    }
}
