<?php

declare(strict_types=1);

namespace Dply\QueueInsights\Transport;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;

/**
 * One message taken off dply's managed queue.
 *
 * Laravel's worker drives this: it asks for the payload, then calls delete() on
 * success or release() on a retry. Both map to one request against the
 * endpoint, which is the whole reason the driver needs no SDK.
 */
class DplyJob extends Job implements JobContract
{
    /**
     * @param  array<string, mixed>  $message
     */
    public function __construct(
        Container $container,
        private readonly DplyQueue $dply,
        private readonly array $message,
        string $connectionName,
        string $queue,
    ) {
        $this->container = $container;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
    }

    public function getJobId(): string
    {
        return (string) ($this->message['MessageId'] ?? '');
    }

    public function getRawBody(): string
    {
        return (string) ($this->message['Body'] ?? '');
    }

    /**
     * How many times this message has been delivered.
     *
     * Comes from the endpoint rather than the payload: a job that timed out
     * mid-run never got to update its own body, and counting deliveries is the
     * only way its next attempt is known to be an attempt.
     */
    public function attempts(): int
    {
        return (int) ($this->message['Attributes']['ApproximateReceiveCount'] ?? 1);
    }

    public function delete(): void
    {
        parent::delete();

        $this->dply->deleteMessage($this->queue, (string) ($this->message['ReceiptHandle'] ?? ''));
    }

    /** @param  int  $delay */
    public function release($delay = 0): void
    {
        parent::release($delay);

        $this->dply->release($this->queue, $this->message, (int) $delay);
    }
}
