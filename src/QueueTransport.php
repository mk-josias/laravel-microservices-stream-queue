<?php

declare(strict_types=1);

namespace StreamQueue;

use Illuminate\Contracts\Queue\Factory as Queue;
use Illuminate\Contracts\Queue\Queue as Connection;
use Microservices\Config\Services;
use Microservices\Contracts\Stream\RedeliversEnvelopes;
use Microservices\Contracts\Stream\Transport;
use Microservices\Data\Envelope;
use Throwable;

/**
 * Events over a Laravel queue connection — database, sqs, beanstalkd — for a stack without Redis.
 *
 * A queue hands each job to one reader, so publishing fans out: one copy per declared service,
 * on its own queue `{key}-{service}`. A failed envelope is released and comes back
 * after the ones queued behind it: this transport does not keep the order across a failure.
 */
final class QueueTransport implements RedeliversEnvelopes, Transport
{
    private const int MAX_BACKOFF_SECONDS = 60;

    private bool $listening = true;

    public function __construct(
        private readonly Queue $queues,
        private readonly QueueStream $config,
        private readonly Services $services,
    ) {}

    public function publish(Envelope $envelope): void
    {
        foreach ($this->services->getDeclared() as $service) {
            if ($envelope->isFor($service)) {
                $this->connection()->pushRaw($envelope->toJson(), $this->queue($service));
            }
        }
    }

    /** Each pass pops one job from every consumer's queue, and sleeps only when all of them were empty. */
    public function consume(array $consumers, array $channels, callable $handle): void
    {
        $failures = array_fill_keys($consumers, 0);

        while ($this->listening) {
            $popped = false;

            foreach ($consumers as $consumer) {
                $job = $this->connection()->pop($this->queue($consumer));

                if ($job === null) {
                    continue;
                }

                $popped = true;
                $envelope = Envelope::fromJson($job->getRawBody());

                if (! in_array($envelope->emitter, $channels, true)) {
                    $job->delete();

                    continue;
                }

                try {
                    $handle($envelope, $consumer);
                } catch (Throwable $e) {
                    report($e);
                    $job->release(min(self::MAX_BACKOFF_SECONDS, 2 ** min(++$failures[$consumer], 6)));

                    continue;
                }

                $job->delete();
                $failures[$consumer] = 0;
            }

            if (! $popped) {
                sleep(max(1, $this->config->getSleep()));
            }
        }
    }

    public function stop(): void
    {
        $this->listening = false;
    }

    private function queue(string $service): string
    {
        return $this->config->getKey().'-'.$service;
    }

    private function connection(): Connection
    {
        return $this->queues->connection($this->config->getConnection());
    }
}
