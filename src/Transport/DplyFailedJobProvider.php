<?php

declare(strict_types=1);

namespace Dply\QueueInsights\Transport;

use Illuminate\Http\Client\PendingRequest;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Support\Facades\Http;

/**
 * Records failed jobs with dply instead of in the app's own database.
 *
 * A managed queue whose failures land in the customer's `failed_jobs` table is
 * half a product: dply can see the depth but has to SSH into the box to say
 * what broke, and an app whose work runs on dply's own workers may have no
 * database of its own to write to.
 *
 * These endpoints are dply-native rather than SQS, so they take the same bearer
 * token as the driver — no signing, and `queue:retry` keeps working because
 * Laravel drives it entirely through this interface.
 */
class DplyFailedJobProvider implements FailedJobProviderInterface
{
    public function __construct(
        private readonly string $url,
        private readonly string $token,
    ) {}

    /**
     * @param  string  $connection
     * @param  string  $queue
     * @param  string  $payload
     * @param  \Throwable  $exception
     */
    public function log($connection, $queue, $payload, $exception): ?string
    {
        $decoded = json_decode($payload, true);
        $uuid = is_array($decoded) ? (string) ($decoded['uuid'] ?? '') : '';

        $response = $this->request()->post($this->url.'/failed-jobs', [
            'uuid' => $uuid,
            'queue' => $queue,
            'payload' => $payload,
            // The whole exception here — class, message and trace. Unlike
            // dply's dashboard read path, this is the app recording its own
            // failure for itself to fetch back.
            'exception' => (string) $exception,
            'attempts' => is_array($decoded) ? (int) ($decoded['attempts'] ?? 0) : 0,
        ]);

        return $response->successful() && $uuid !== '' ? $uuid : null;
    }

    /**
     * @return array<int, object>
     */
    public function all(): array
    {
        $response = $this->request()->get($this->url.'/failed-jobs', ['limit' => 100]);

        if ($response->failed()) {
            return [];
        }

        return array_map(
            static fn (array $row): object => (object) $row,
            (array) $response->json('failed_jobs', []),
        );
    }

    /** @param  mixed  $id */
    public function find($id): ?object
    {
        $response = $this->request()->get($this->url.'/failed-jobs/'.rawurlencode((string) $id));

        if ($response->failed()) {
            return null;
        }

        $row = (array) $response->json();

        return $row === [] ? null : (object) $row;
    }

    /** @param  mixed  $id */
    public function forget($id): bool
    {
        $response = $this->request()->delete($this->url.'/failed-jobs/'.rawurlencode((string) $id));

        return $response->successful() && (bool) $response->json('forgotten', false);
    }

    /** @param  int|null  $hours */
    public function flush($hours = null): void
    {
        $this->request()->post($this->url.'/failed-jobs/flush', $hours !== null ? ['hours' => (int) $hours] : []);
    }

    /**
     * @param  string|null  $queue
     * @return array<int, mixed>
     */
    public function ids($queue = null): array
    {
        return array_values(array_filter(array_map(
            static fn (object $row): ?string => isset($row->id) ? (string) $row->id : null,
            $this->all(),
        )));
    }

    private function request(): PendingRequest
    {
        return Http::withToken($this->token)
            ->acceptJson()
            ->timeout(10)
            ->retry(2, 200, throw: false);
    }
}
