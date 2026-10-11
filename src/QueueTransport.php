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

    public function consume(string $consumer, array $channels, callable $handle): void
    {
        $failures = 0;

        while ($this->listening) {
            $job = $this->connection()->pop($this->queue($consumer));

            if ($job === null) {
                sleep(max(1, $this->config->getSleep()));

                continue;
            }

            $envelope = Envelope::fromJson($job->getRawBody());

            if (! in_array($envelope->emitter, $channels, true)) {
                $job->delete();

                continue;
            }

            try {
                $handle($envelope);
            } catch (Throwable $e) {
                report($e);
                $job->release(min(self::MAX_BACKOFF_SECONDS, 2 ** min(++$failures, 6)));

                continue;
            }

            $job->delete();
            $failures = 0;
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
