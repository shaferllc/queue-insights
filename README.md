# dply Queue Insights

Reports Laravel queue job lifecycle events to [dply](https://dply.io).

Queue **depth**, **waiting jobs** and **failures** can all be read straight from
the queue store, and dply reads them without this package. What no external
observer can see is **execution**: how long a job took, whether it succeeded,
and what a job class's throughput looks like. A completed job deletes its own
row, so by the time anything polls, the evidence is gone.

Only the application is present at that moment. That is what this package is
for, and it is the only thing it does.

## Install

```bash
composer require dply/queue-insights
```

The service provider is auto-discovered. **The package is inert until dply
configures an endpoint and token** — with neither set it registers no listeners
at all, so installing it costs a stopped application nothing.

dply installs and configures this for you when queue insights are enabled for a
site. You do not normally require it by hand.

## Configuration

| env | default | meaning |
| --- | --- | --- |
| `DPLY_QUEUE_INSIGHTS_ENDPOINT` | — | Where events are POSTed. Unset disables the package. |
| `DPLY_QUEUE_INSIGHTS_TOKEN` | — | Bearer token for that endpoint. Unset disables the package. |
| `DPLY_QUEUE_INSIGHTS_BUFFER` | `50` | Events held in memory before the oldest are dropped. |
| `DPLY_QUEUE_INSIGHTS_TIMEOUT` | `2.0` | Seconds allowed for the flush request. |

## What it sends

For each job: name, queue, connection, attempt count, and duration in
milliseconds. For failures, additionally the exception **class** and the first
500 characters of its message.

It does **not** send job payloads. A payload carries whatever your application
was handed — often customer data — and shipping that off the box to make a
dashboard prettier is not a reasonable trade.

## Design rules

The package runs inside your queue workers, so it is built to be impossible to
notice:

1. **It never throws.** An exception raised in a queue-event listener fails the
   *job*. Monitoring must not cause the outage it reports.
2. **It never blocks.** Events buffer in memory and flush once, on shutdown. A
   slow or dead endpoint costs a worker one timeout per *process*, not per job.
3. **It never grows without bound.** A full buffer drops the oldest events
   rather than consuming memory a worker is running under a `--memory` cap for.

## Requirements

PHP 8.2+, Laravel 11, 12 or 13.

## License

MIT.
