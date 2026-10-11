<?php

declare(strict_types=1);

namespace StreamQueue;

use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Queue\Factory as Queue;
use Illuminate\Support\ServiceProvider;
use Microservices\Config\Services;
use Microservices\Services\Stream\TransportManager;

final class StreamQueueServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->callAfterResolving(TransportManager::class, static function (TransportManager $transports): void {
            $transports->extend('queue', static fn (Container $app, array $options, string $stream): QueueTransport => new QueueTransport(
                $app->make(Queue::class),
                new QueueStream($app->make(Repository::class), $stream),
                $app->make(Services::class),
            ));
        });
    }
}
