<?php

declare(strict_types=1);

namespace Dply\QueueInsights;

/**
 * Ships job events to dply without ever getting in a job's way.
 *
 * Three rules, in order of importance:
 *
 *   1. Never throw. An exception raised from a queue-event listener fails the
 *      JOB, which would mean monitoring causing the outage it reports.
 *   2. Never block. Events buffer in memory and flush once, on shutdown, so a
 *      slow or dead endpoint costs the worker one timeout per process rather
 *      than one per job.
 *   3. Never grow without bound. A full buffer drops the oldest events instead
 *      of consuming the memory a worker is running under a --memory limit for.
 */
class Reporter
{
    /** @var list<array<string, mixed>> */
    private array $buffer = [];

    private bool $flushRegistered = false;

    public function __construct(
        private readonly string $endpoint,
        private readonly string $token,
        private readonly int $limit = 50,
        private readonly float $timeout = 2.0,
    ) {}

    public function isConfigured(): bool
    {
        return $this->endpoint !== '' && $this->token !== '';
    }

    /**
     * @param  array<string, mixed>  $payload
     */
    public function record(string $event, array $payload): void
    {
        if (! $this->isConfigured()) {
            return;
        }

        $this->buffer[] = ['event' => $event, 'at' => microtime(true)] + $payload;

        // Drop oldest, not newest: the recent events describe what is happening
        // now, which is what anyone looking at the dashboard is asking about.
        if (count($this->buffer) > $this->limit) {
            array_shift($this->buffer);
        }

        if (! $this->flushRegistered) {
            $this->flushRegistered = true;
            register_shutdown_function(fn () => $this->flush());
        }
    }

    public function flush(): void
    {
        if ($this->buffer === [] || ! $this->isConfigured()) {
            return;
        }

        $body = json_encode(['events' => $this->buffer]);
        $this->buffer = [];

        if ($body === false) {
            return;
        }

        try {
            $ch = curl_init($this->endpoint);

            if ($ch === false) {
                return;
            }

            curl_setopt_array($ch, [
                CURLOPT_POST => true,
                CURLOPT_POSTFIELDS => $body,
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_TIMEOUT => (int) ceil($this->timeout),
                CURLOPT_CONNECTTIMEOUT => 2,
                CURLOPT_HTTPHEADER => [
                    'Content-Type: application/json',
                    'Authorization: Bearer '.$this->token,
                ],
            ]);

            curl_exec($ch);
            curl_close($ch);
        } catch (\Throwable) {
            // Rule 1. There is no error path worth taking here: the worker's job
            // is to process jobs, and dply losing a metrics batch is not the
            // worker's problem.
        }
    }
}
