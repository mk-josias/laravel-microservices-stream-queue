# Queue Event Stream

The `queue` events driver of [laravel-microservices](https://github.com/mk-josias/laravel-microservices/tree/main/packages/core), on any Laravel queue
connection (`database`, `sqs`, `beanstalkd`), for a stack without Redis.

```bash
composer require mk-josias/laravel-microservices-stream-queue
```

```php
// config/microservices.php
'default' => [
    'driver' => 'queue',
    // a queue.connections entry, null for the default one
    'connection' => env('MICROSERVICES_QUEUE_CONNECTION'),
    // each service reads {key}-{service}
    'key' => 'microservices-events',
    // seconds to wait when the queue is empty
    'sleep' => 1,
],
```

| What it guarantees | |
|---|---|
| A copy per consumer | each envelope is copied to one queue per declared service it is for |
| At least once | a copy is deleted after its handlers succeed; a failed one is released and comes back |
| Order | kept while nothing fails. A failed envelope comes back **after** the ones queued behind it |

Choose `redis` when a consumer depends on the order of two events, for example a copy that must
exist before the event that reads it.
