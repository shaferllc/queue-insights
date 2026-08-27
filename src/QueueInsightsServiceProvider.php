<?php

declare(strict_types=1);

namespace Dply\QueueInsights;

use Dply\QueueInsights\Transport\DplyConnector;
use Dply\QueueInsights\Transport\DplyFailedJobProvider;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;

/**
 * Reports queue job lifecycle to dply.
 *
 * Everything dply can learn by reading a queue store — depth, waiting jobs,
 * failures — it already reads without this package. What it CANNOT see from
 * outside is execution: how long a job took, whether it succeeded, and what the
 * throughput of a job class is. A completed job deletes its own row, so by the
 * time anything polls, the evidence is gone. Only the app is present at that
 * moment, which is why this listens rather than dply polling harder.
 *
 * Laravel's own queue events are the whole integration surface — no middleware,
 * no wrapper around the worker, nothing to keep in step with a Laravel release.
 *
 * Auto-discovered via extra.laravel.providers, and inert unless dply has
 * configured an endpoint and token, so installing it changes nothing on its own.
 */
class QueueInsightsServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/queue-insights.php', 'queue-insights');

        $this->registerManagedQueueConnection();

        $this->app->singleton(Reporter::class, fn (): Reporter => new Reporter(
            (string) config('queue-insights.endpoint', ''),
            (string) config('queue-insights.token', ''),
        ));
    }

    /**
     * Register the `dply` queue connection and its driver.
     *
     * NOT the `sqs` driver. dply's endpoint speaks the SQS wire protocol, so
     * that would work — but it pulls aws/aws-sdk-php into an app that has no
     * business with Amazon, and the stock `sqs` block reads AWS_ACCESS_KEY_ID,
     * which most apps also use for S3. Pointing those at dply would break the
     * filesystem disk to fix the queue.
     *
     * The endpoint takes a bearer token as readily as a signature, so the
     * driver here is plain HTTP: no SDK, no SigV4 signer, no AWS-shaped
     * variables. Nothing registers unless DPLY_QUEUE_URL is set.
     */
    private function registerManagedQueueConnection(): void
    {
        $url = trim((string) env('DPLY_QUEUE_URL', ''));

        if ($url === '') {
            return;
        }

        // Never clobber a connection the app declared itself.
        if (! is_array(config('queue.connections.dply'))) {
            config([
                'queue.connections.dply' => [
                    'driver' => 'dply',
                    'url' => rtrim($url, '/'),
                    'token' => (string) env('DPLY_QUEUE_TOKEN', ''),
                    'queue' => (string) env('DPLY_QUEUE_DEFAULT', 'default'),
                    'retry_after' => (int) env('DPLY_QUEUE_RETRY_AFTER', 90),
                    'after_commit' => false,
                ],
            ]);
        }

        $this->app->afterResolving('queue', function ($manager): void {
            $manager->addConnector('dply', fn (): DplyConnector => new DplyConnector);
        });

        // Failures follow the jobs — but only when asked. Laravel defaults this
        // to `database-uuids`, and an app may well depend on its own
        // failed_jobs table, so QUEUE_FAILED_DRIVER=dply is the opt-in rather
        // than something the package assumes because a queue moved.
        if ((string) config('queue.failed.driver', '') === 'dply') {
            $this->app->singleton('queue.failer', fn (): DplyFailedJobProvider => new DplyFailedJobProvider(
                rtrim($url, '/'),
                (string) env('DPLY_QUEUE_TOKEN', ''),
            ));
        }
    }

    public function boot(): void
    {
        $reporter = $this->app->make(Reporter::class);

        if (! $reporter->isConfigured()) {
            // No endpoint means no listeners at all: an unconfigured agent must
            // cost the app nothing, not merely do nothing.
            return;
        }

        // Wall-clock per job, keyed by job id. A worker handles one job at a
        // time, but keying by id rather than using a single property keeps this
        // correct if that ever stops being true.
        $started = [];

        Event::listen(JobQueued::class, static function (JobQueued $event) use ($reporter): void {
            $reporter->record('queued', [
                'id' => (string) $event->id,
                'name' => self::name($event->job),
                'queue' => $event->job->queue ?? null,
                'connection' => $event->connectionName,
            ]);
        });

        Event::listen(JobProcessing::class, static function (JobProcessing $event) use (&$started): void {
            $started[$event->job->getJobId()] = microtime(true);
        });

        Event::listen(JobProcessed::class, static function (JobProcessed $event) use ($reporter, &$started): void {
            $id = $event->job->getJobId();

            $reporter->record('processed', [
                'id' => (string) $id,
                'name' => $event->job->resolveName(),
                'queue' => $event->job->getQueue(),
                'connection' => $event->connectionName,
                'duration_ms' => self::elapsed($started, $id),
                'attempts' => $event->job->attempts(),
            ]);

            unset($started[$id]);
        });

        Event::listen(JobFailed::class, static function (JobFailed $event) use ($reporter, &$started): void {
            $id = $event->job->getJobId();

            $reporter->record('failed', [
                'id' => (string) $id,
                'name' => $event->job->resolveName(),
                'queue' => $event->job->getQueue(),
                'connection' => $event->connectionName,
                'duration_ms' => self::elapsed($started, $id),
                'attempts' => $event->job->attempts(),
                // Class and message only. A payload can carry anything the app
                // was handed, and shipping customer data off the box to make a
                // dashboard prettier is not a trade dply gets to make.
                'exception' => get_class($event->exception),
                'message' => mb_substr($event->exception->getMessage(), 0, 500),
            ]);

            unset($started[$id]);
        });
    }

    /** @param array<string, float> $started */
    private static function elapsed(array $started, ?string $id): ?int
    {
        return isset($started[$id]) ? (int) round((microtime(true) - $started[$id]) * 1000) : null;
    }

    private static function name(mixed $job): string
    {
        return is_object($job) ? $job::class : (string) $job;
    }
}
