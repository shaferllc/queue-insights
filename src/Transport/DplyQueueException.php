<?php

declare(strict_types=1);

namespace Dply\QueueInsights\Transport;

use RuntimeException;

/**
 * The endpoint refused the request, or could not be reached.
 *
 * Its own class so an app can catch queue transport trouble specifically:
 * "dply is unreachable" and "this job threw" call for different responses.
 */
class DplyQueueException extends RuntimeException {}
