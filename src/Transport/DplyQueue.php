<?php

declare(strict_types=1);

namespace Dply\QueueInsights\Transport;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Facades\Http;

/**
 * Laravel queue driver for dply's managed queue.
 *
 * dply's endpoint speaks the SQS wire protocol, so Laravel's own `sqs` driver
 * would talk to it. This exists because of what that costs the app: the AWS SDK
 * as a dependency, and the stock `sqs` connection reading AWS_ACCESS_KEY_ID —
 * the same variable the app's S3 disk uses. An app should not have to choose
 * between its filesystem and its queue, and should not carry Amazon's SDK to
 * reach a service that is not Amazon's.
 *
 * The endpoint accepts a bearer token in place of a signature, so all this
 * needs is an HTTP client. Three requests make a working queue: push, receive,
 * delete — the rest is Laravel's own serialization, untouched.
 */
class DplyQueue extends Queue implements QueueContract
{
    public function __construct(
        private readonly string $url,
        private readonly string $token,
        private readonly string $default,
        private readonly int $retryAfter = 90,
    ) {}

    /**
     * Total depth: waiting, in flight and scheduled.
     *
     * Laravel's own definition of size() for a driver that can see all three —
     * counting only what is visible would report an empty queue while a worker
     * was mid-job on the last one.
     *
     * @param  string|null  $queue
     */
    public function size($queue = null): int
    {
        $a = $this->attributes($queue);

        return (int) ($a['ApproximateNumberOfMessages'] ?? 0)
            + (int) ($a['ApproximateNumberOfMessagesNotVisible'] ?? 0)
            + (int) ($a['ApproximateNumberOfMessagesDelayed'] ?? 0);
    }

    /** @param  string|null  $queue */
    public function pendingSize($queue = null): int
    {
        return (int) ($this->attributes($queue)['ApproximateNumberOfMessages'] ?? 0);
    }

    /** @param  string|null  $queue */
    public function delayedSize($queue = null): int
    {
        return (int) ($this->attributes($queue)['ApproximateNumberOfMessagesDelayed'] ?? 0);
    }

    /** @param  string|null  $queue */
    public function reservedSize($queue = null): int
    {
        return (int) ($this->attributes($queue)['ApproximateNumberOfMessagesNotVisible'] ?? 0);
    }

    /**
     * Age of the oldest waiting job.
     *
     * Null, honestly: the endpoint reports depth, not enqueue times, and
     * inventing a timestamp here would put a made-up number on Laravel's own
     * queue-health output. dply's dashboard reads this from its own store,
     * where the answer actually exists.
     *
     * @param  string|null  $queue
     */
    public function creationTimeOfOldestPendingJob($queue = null): ?int
    {
        return null;
    }

    /**
     * @param  string|null  $queue
     * @return array<string, mixed>
     */
    private function attributes($queue): array
    {
        $response = $this->call($queue, 'GetQueueAttributes', [
            'AttributeNames' => [
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible',
                'ApproximateNumberOfMessagesDelayed',
            ],
        ]);

        return (array) ($response['Attributes'] ?? []);
    }

    /**
     * @param  string|object  $job
     * @param  mixed  $data
     * @param  string|null  $queue
     */
    public function push($job, $data = '', $queue = null): mixed
    {
        return $this->pushRaw($this->createPayload($job, $this->getQueue($queue), $data), $queue);
    }

    /**
     * @param  string  $payload
     * @param  string|null  $queue
     * @param  array<string, mixed>  $options
     */
    public function pushRaw($payload, $queue = null, array $options = []): mixed
    {
        $response = $this->call($queue, 'SendMessage', ['MessageBody' => $payload]);

        return $response['MessageId'] ?? null;
    }

    /**
     * @param  \DateTimeInterface|\DateInterval|int  $delay
     * @param  string|object  $job
     * @param  mixed  $data
     * @param  string|null  $queue
     */
    public function later($delay, $job, $data = '', $queue = null): mixed
    {
        $response = $this->call($queue, 'SendMessage', [
            'MessageBody' => $this->createPayload($job, $this->getQueue($queue), $data),
            'DelaySeconds' => $this->secondsUntil($delay),
        ]);

        return $response['MessageId'] ?? null;
    }

    /** @param  string|null  $queue */
    public function pop($queue = null): ?DplyJob
    {
        $response = $this->call($queue, 'ReceiveMessage', [
            'MaxNumberOfMessages' => 1,
            'AttributeNames' => ['ApproximateReceiveCount'],
        ]);

        $message = $response['Messages'][0] ?? null;

        if (! is_array($message)) {
            return null;
        }

        return new DplyJob(
            $this->container,
            $this,
            $message,
            $this->connectionName,
            $this->getQueue($queue),
        );
    }

    /**
     * Hand a job back so it becomes visible again after $delay seconds.
     *
     * @param  array<string, mixed>  $message
     */
    public function release(string $queue, array $message, int $delay): void
    {
        $this->call($queue, 'ChangeMessageVisibility', [
            'ReceiptHandle' => (string) ($message['ReceiptHandle'] ?? ''),
            'VisibilityTimeout' => max(0, $delay),
        ]);
    }

    public function deleteMessage(string $queue, string $receipt): void
    {
        $this->call($queue, 'DeleteMessage', ['ReceiptHandle' => $receipt]);
    }

    public function getQueue(?string $queue): string
    {
        return $queue !== null && $queue !== '' ? $queue : $this->default;
    }

    public function retryAfter(): int
    {
        return $this->retryAfter;
    }

    /**
     * One request shape for every operation.
     *
     * A failure throws rather than returning empty: a queue that silently
     * reported zero when the endpoint was unreachable would let a worker idle
     * through an outage looking healthy, and a push that quietly vanished would
     * lose the job.
     *
     * @param  array<string, mixed>  $payload
     * @return array<string, mixed>
     */
    private function call(?string $queue, string $action, array $payload): array
    {
        $response = Http::withToken($this->token)
            ->withHeaders([
                // The endpoint dispatches on this header, the same as it does
                // for a signed SQS client.
                'X-Amz-Target' => 'AmazonSQS.'.$action,
                'Content-Type' => 'application/x-amz-json-1.0',
            ])
            ->timeout(15)
            ->retry(2, 200, throw: false)
            ->post($this->url.'/'.$this->getQueue($queue), $payload);

        if ($response->failed()) {
            throw new DplyQueueException(
                'dply Queue '.$action.' failed ('.$response->status().'): '.mb_substr($response->body(), 0, 300)
            );
        }

        return (array) $response->json();
    }
}
