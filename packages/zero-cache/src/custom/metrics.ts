import {
  getOrCreateCounter,
  getOrCreateHistogram,
  getOrCreateUpDownCounter,
} from '../observability/metrics.ts';

export type ApiOperation = 'mutate' | 'query' | 'cleanup' | 'validate_auth';
export type ApiCleanupType = 'single' | 'bulk';

export type ApiMetricBaseAttrs = {
  operation: ApiOperation;
  cleanup_type?: ApiCleanupType | undefined;
};

export type ApiRequestResult =
  | 'success'
  | 'api_error'
  | 'http_error'
  | 'parse_error'
  | 'fetch_error'
  | 'url_not_allowed'
  | 'config_error';

export type ApiAttemptResult =
  | 'success'
  | 'api_error'
  | 'http_error'
  | 'parse_error'
  | 'fetch_error';

export type ApiRequestMetricAttrs = ApiMetricBaseAttrs & {
  result: ApiRequestResult;
  attempt_count: number;
  http_status_code?: number | undefined;
  http_status_class?: `${string}xx` | undefined;
  error_kind?: string | undefined;
  error_reason?: string | undefined;
};

export type ApiAttemptMetricAttrs = ApiMetricBaseAttrs & {
  attempt: number;
  result: ApiAttemptResult;
  will_retry: boolean;
  http_status_code?: number | undefined;
  http_status_class?: `${string}xx` | undefined;
  error_kind?: string | undefined;
  error_reason?: string | undefined;
};

const API_DURATION_HISTOGRAM_BOUNDARIES_S = [
  0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60,
  120,
];

export function apiRequests() {
  return getOrCreateCounter(
    'server',
    'api.requests',
    'API requests, labeled by operation and result.',
  );
}

export function apiRequestDuration() {
  return getOrCreateHistogram('server', 'api.request_duration', {
    description: 'End-to-end API request duration, including retries.',
    unit: 's',
    bucketBoundaries: API_DURATION_HISTOGRAM_BOUNDARIES_S,
  });
}

export function apiAttempts() {
  return getOrCreateCounter(
    'server',
    'api.attempts',
    'API HTTP fetch attempts',
  );
}

export function apiAttemptDuration() {
  return getOrCreateHistogram('server', 'api.attempt_duration', {
    description: 'API HTTP fetch attempt duration, excluding retry sleep.',
    unit: 's',
    bucketBoundaries: API_DURATION_HISTOGRAM_BOUNDARIES_S,
  });
}

export function apiInFlight() {
  return getOrCreateUpDownCounter(
    'server',
    'api.in_flight',
    'API requests currently in flight.',
  );
}
