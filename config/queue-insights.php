<?php

declare(strict_types=1);

return [
    // Both injected by dply at deploy time. Absent means the agent does nothing
    // and registers no listeners.
    'endpoint' => env('DPLY_QUEUE_INSIGHTS_ENDPOINT', ''),
    'token' => env('DPLY_QUEUE_INSIGHTS_TOKEN', ''),

    // Reporting must never slow a job down or fail one. Events are buffered and
    // flushed on shutdown; a full buffer drops rather than blocks.
    'buffer' => (int) env('DPLY_QUEUE_INSIGHTS_BUFFER', 50),
    'timeout' => (float) env('DPLY_QUEUE_INSIGHTS_TIMEOUT', 2.0),
];
