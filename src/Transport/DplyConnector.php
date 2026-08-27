<?php

declare(strict_types=1);

namespace Dply\QueueInsights\Transport;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Connectors\ConnectorInterface;

/**
 * Builds {@see DplyQueue} from a `dply` connection block.
 *
 * Deliberately tolerant about a missing token: the app should fail when it
 * tries to USE the queue, carrying the endpoint's own message, rather than at
 * boot with a config error that takes the whole site down over a queue this
 * request might never touch.
 */
class DplyConnector implements ConnectorInterface
{
    /** @param  array<string, mixed>  $config */
    public function connect(array $config): QueueContract
    {
        return new DplyQueue(
            rtrim((string) ($config['url'] ?? ''), '/'),
            (string) ($config['token'] ?? ''),
            (string) ($config['queue'] ?? 'default'),
            (int) ($config['retry_after'] ?? 90),
        );
    }
}
