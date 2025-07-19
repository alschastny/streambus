<?php

declare(strict_types=1);

namespace StreamBus\Processor;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use StreamBus\Consumer\StreamBusConsumerInterface;

final class StreamBusProcessor
{
    private int $batch = 1;
    private ?int $blockMs = null;
    private bool $ack = true;
    private bool $nack = true;
    /** @var array<string, callable> $handlers */
    private array $handlers = [];
    /** @var callable|null $emptyHandler */
    private $emptyHandler = null;
    /** @var callable|null $interruptCallback */
    private $interruptCallback = null;
    private LoggerInterface $logger;

    public function __construct(private readonly StreamBusConsumerInterface $consumer)
    {
        $this->logger = new NullLogger();
    }

    public function setLogger(LoggerInterface $logger): self
    {
        $this->logger = $logger;
        return $this;
    }

    public function setHandler(string $subject, callable $handler): self
    {
        $this->handlers[$subject] = $handler;
        return $this;
    }

    public function setHandlers(array $handlers): self
    {
        $this->handlers = $handlers;
        return $this;
    }

    public function setEmptyHandler(?callable $handler): self
    {
        $this->emptyHandler = $handler;
        return $this;
    }

    public function setInterruptCallback(?callable $interruptCallback): self
    {
        $this->interruptCallback = $interruptCallback;
        return $this;
    }

    public function setReadBatch(int $batch): self
    {
        $this->batch = $batch;
        return $this;
    }

    public function setBlockMs(?int $blockMs): self
    {
        $this->blockMs = $blockMs;
        return $this;
    }

    public function setAckMode(bool $ack, bool $nack): self
    {
        $this->ack = $ack;
        $this->nack = $nack;
        return $this;
    }

    /**
     * @throws \Throwable
     */
    public function process(int $iterations): int
    {
        $processed = 0;
        $this->logger->info('start', ['iterations' => $iterations]);
        for ($i = 1; $i <= $iterations; $i++) {
            $this->logger->debug('iteration', ['i' => $i]);
            if ($this->interruptCallback && ($this->interruptCallback)()) {
                $this->logger->info('interrupted');
                break;
            }
            if (!$read = $this->consumer->read($this->batch, $this->blockMs)) {
                $this->logger->debug('nothing read');
                continue;
            }
            foreach ($read as $subject => $messages) {
                $this->logger->info('handling', ['subject' => $subject, 'count' => count($messages)]);
                $handler = $this->handlers[$subject] ?? throw new StreamBusProcessorException('unknown message subject: ' . $subject);
                $success = $fail = 0;
                foreach ($messages as $id => $message) {
                    $processed++;
                    try {
                        if ($message === null) {
                            $this->logger->warning('message is NULL', ['subject' => $subject, 'id' => $id]);
                            $result = ($this->emptyHandler ?? $handler)($subject, $id, $message);
                        } else {
                            $result = $handler($subject, $id, $message);
                        }
                        if ($result) {
                            $success++;
                            if ($this->ack) {
                                $this->consumer->ack($subject, $id);
                                $this->logger->debug('ack', ['subject' => $subject, 'id' => $id]);
                            }
                        } else {
                            $fail++;
                            if ($this->nack) {
                                $this->consumer->nack($subject, $id);
                                $this->logger->debug('nack', ['subject' => $subject, 'id' => $id]);
                            } else {
                                throw new StreamBusProcessorException('message not processed ' . $subject . ', id: ' . $id);
                            }
                        }
                    } catch (\Throwable $e) {
                        $this->logger->error('exception', ['subject' => $subject, 'id' => $id, 'exception' => $e]);
                        if ($this->nack) {
                            $this->logger->debug('nack', ['subject' => $subject, 'id' => $id]);
                            $this->consumer->nack($subject, $id);
                        }
                        throw $e;
                    }
                }
                $this->logger->info('handled', ['subject' => $subject, 'count' => count($messages), 'success' => $success, 'fail' => $fail]);
            }
        }
        $this->logger->info('finish', ['processed' => $processed, 'iterations' => $i]);
        return $processed;
    }
}
