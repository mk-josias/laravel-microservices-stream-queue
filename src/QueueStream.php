<?php

declare(strict_types=1);

namespace StreamQueue;

use Illuminate\Contracts\Config\Repository;

/** The microservices.events.streams.{name}.* settings of a stream on the `queue` driver; read live, never snapshotted. */
final readonly class QueueStream
{
    public function __construct(private Repository $config, public string $name) {}

    /** Null is the application's default queue connection. */
    public function getConnection(): ?string
    {
        $connection = $this->config->get("microservices.events.streams.{$this->name}.connection");

        return is_string($connection) && $connection !== '' ? $connection : null;
    }

    /** Each service reads its own queue, {key}-{service}. */
    public function getKey(): string
    {
        return (string) $this->config->get("microservices.events.streams.{$this->name}.key", "microservices-{$this->name}");
    }

    public function getSleep(): int
    {
        return (int) $this->config->get("microservices.events.streams.{$this->name}.sleep", 1);
    }
}
