<?php

declare(strict_types=1);

namespace StreamQueue\Tests;

use Microservices\Tests\TestCase as CoreTestCase;
use StreamQueue\StreamQueueServiceProvider;

/** The core's `orders` application, with the queue driver installed. */
abstract class TestCase extends CoreTestCase
{
    /** @return list<class-string> */
    protected function getPackageProviders($app): array
    {
        return [...parent::getPackageProviders($app), StreamQueueServiceProvider::class];
    }
}
