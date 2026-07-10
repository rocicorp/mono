import {context, propagation} from '@opentelemetry/api';
import type {LogContext, LogLevel} from '@rocicorp/logger';
import 'urlpattern-polyfill';
import {unreachable} from '../../../shared/src/asserts.ts';
import {getErrorMessage} from '../../../shared/src/error.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {type Type} from '../../../shared/src/valita.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {
  errorBodySchema,
  isProtocolError,
  type ErrorBody,
} from '../../../zero-protocol/src/error.ts';
import {
  pushErrorSchema,
  type PushError,
} from '../../../zero-protocol/src/push.ts';
import type {ConnectionContext} from '../services/view-syncer/connection-context-manager.ts';
import {ProtocolErrorWithLevel} from '../types/error-with-level.ts';
import {upstreamSchema, type ShardID} from '../types/shards.ts';
import {
  apiAttemptDuration,
  apiAttempts,
  apiInFlight,
  apiRequestDuration,
  apiRequests,
  type ApiAttemptMetricAttrs,
  type ApiAttemptResult,
  type ApiCleanupType,
  type ApiMetricBaseAttrs,
  type ApiOperation,
  type ApiRequestMetricAttrs,
  type ApiRequestResult,
} from './metrics.ts';

const reservedParams = ['schema', 'appID'];

/**
 * Compiles and validates a URLPattern from configuration.
 *
 * Patterns must be full URLs (e.g., "https://api.example.com/endpoint").
 * URLPattern automatically sets search and hash to wildcard ('*'),
 * which means query parameters and fragments are ignored during matching.
 *
 * @throws Error if the pattern is an invalid URLPattern
 */
export function compileUrlPattern(pattern: string): URLPattern {
  try {
    return new URLPattern(pattern);
  } catch (e) {
    throw new Error(
      `Invalid URLPattern in URL configuration: "${pattern}". Error: ${e instanceof Error ? e.message : String(e)}`,
    );
  }
}

export const getBodyPreview = async (
  res: Response,
  lc: LogContext,
): Promise<string | undefined> => {
  try {
    const body = await res.clone().text();
    if (body.length > 512) {
      return body.slice(0, 512) + '...';
    }
    return body;
  } catch (e) {
    lc.warn?.(
      'failed to get body preview',
      {
        url: res.url,
      },
      e,
    );
  }

  return undefined;
};

const MAX_ATTEMPTS = 4;

type ApiFailedReason =
  | typeof ErrorReason.HTTP
  | typeof ErrorReason.Parse
  | typeof ErrorReason.Internal;

export type FetchMetricsOptions = {
  operation: ApiOperation;
  cleanupType?: ApiCleanupType | undefined;
};

export async function fetchFromAPIServer<TValidator extends Type>(
  validator: TValidator,
  source: 'push' | 'transform',
  lc: LogContext,
  ctx: ConnectionContext,
  shard: ShardID,
  body: ReadonlyJSONValue,
  metricsOpts: FetchMetricsOptions,
) {
  const metricAttrs: ApiMetricBaseAttrs =
    metricsOpts.operation === 'cleanup' && metricsOpts.cleanupType !== undefined
      ? {
          operation: metricsOpts.operation,
          cleanup_type: metricsOpts.cleanupType,
        }
      : {operation: metricsOpts.operation};
  const requestStart = performance.now();
  let requestMetricAttrs: ApiRequestMetricAttrs | undefined;
  let attemptCount = 0;
  apiInFlight().add(1, metricAttrs);

  try {
    const fetchFromAPIServerID = randInt(1, Number.MAX_SAFE_INTEGER).toString(
      36,
    );
    lc = lc
      .withContext('fetchFromAPIServerID', fetchFromAPIServerID)
      .withContext('source', source);

    const fetchConfig =
      source === 'push' ? ctx.mutateContext : ctx.queryContext;
    const url = must(
      fetchConfig.url,
      `Fetch config for ${source} is missing URL`,
    );
    const headerOptions = fetchConfig.headerOptions;

    lc.debug?.('fetchFromAPIServer called', {
      url,
    });

    if (!urlMatch(url, fetchConfig.allowedUrlPatterns ?? [])) {
      const errorBody = apiFailedBody(
        source,
        ErrorReason.Internal,
        source === 'push'
          ? `URL "${url}" is not allowed by the ZERO_MUTATE_URL configuration`
          : `URL "${url}" is not allowed by the ZERO_QUERY_URL configuration`,
      );
      requestMetricAttrs = apiRequestMetricAttrs(metricAttrs, {
        result: 'url_not_allowed',
        attemptCount,
        errorBody,
      });
      throw new ProtocolErrorWithLevel(errorBody, 'warn');
    }
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    if (headerOptions.apiKey) {
      headers['X-Api-Key'] = headerOptions.apiKey;
    }
    Object.assign(
      headers,
      headerOptions.customHeaders,
      headerOptions.requestHeaders,
    );
    if (ctx.auth?.raw) {
      headers['Authorization'] = `Bearer ${ctx.auth.raw}`;
    }
    if (headerOptions.cookie) {
      headers['Cookie'] = headerOptions.cookie;
    }
    if (headerOptions.origin) {
      headers['Origin'] = headerOptions.origin;
    }
    propagation.inject(context.active(), headers);

    const urlObj = new URL(url);
    const params = new URLSearchParams(urlObj.search);

    for (const reserved of reservedParams) {
      if (params.has(reserved)) {
        requestMetricAttrs = apiRequestMetricAttrs(metricAttrs, {
          result: 'config_error',
          attemptCount,
        });
        throw new Error(
          `The push URL cannot contain the reserved query param "${reserved}"`,
        );
      }
    }

    params.append('schema', upstreamSchema(shard));
    params.append('appID', shard.appID);

    urlObj.search = params.toString();

    const finalUrl = urlObj.toString();

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      attemptCount = attempt;
      lc = lc.withContext('fetchFromAPIServerAttempt', attempt);
      lc.debug?.('fetch from API server attempt');
      const sleepBeforeRetry = async () => {
        if (attempt < MAX_ATTEMPTS) {
          const delayMs = getBackoffDelayMs(attempt);
          lc.debug?.(`fetch from API server retrying in ${delayMs} ms`);
          await sleep(delayMs);
          return true;
        }
        lc.debug?.('fetch from API server reached max attempts, not retrying');
        return false;
      };
      const attemptStart = performance.now();
      try {
        const response = await fetch(finalUrl, {
          method: 'POST',
          headers,
          body: JSON.stringify(body),
        });

        if (!response.ok) {
          const bodyPreview = await getBodyPreview(response, lc);
          lc.warn?.('fetch from API server returned non-OK status', {
            url: finalUrl,
            status: response.status,
            bodyPreview,
          });
          // Bad Gateway or Gateway Timeout indicate the server was not reached
          // We retry these if we have retries remaining.
          const willRetry =
            (response.status === 502 || response.status === 504) &&
            attempt < MAX_ATTEMPTS;
          recordApiAttempt(performance.now() - attemptStart, metricAttrs, {
            attempt,
            result: 'http_error',
            willRetry,
            response,
          });
          if (willRetry && (await sleepBeforeRetry())) {
            continue;
          }

          const errorBody = apiFailedBody(
            source,
            ErrorReason.HTTP,
            `Fetch from API server returned non-OK status ${response.status}`,
            response,
            bodyPreview,
          );
          requestMetricAttrs = apiRequestMetricAttrs(metricAttrs, {
            result: 'http_error',
            attemptCount,
            response,
            errorBody,
          });
          throw new ProtocolErrorWithLevel(errorBody, 'warn');
        }

        try {
          const json = await response.json();
          const result = validator.parse(json, {
            mode: 'passthrough',
          });
          const apiError = apiErrorFromResult(result);
          if (apiError) {
            recordApiAttempt(performance.now() - attemptStart, metricAttrs, {
              attempt,
              result: 'api_error',
              willRetry: false,
              response,
              errorBody: apiError,
            });
            requestMetricAttrs = apiRequestMetricAttrs(metricAttrs, {
              result: 'api_error',
              attemptCount,
              response,
              errorBody: apiError,
            });
          } else {
            recordApiAttempt(performance.now() - attemptStart, metricAttrs, {
              attempt,
              result: 'success',
              willRetry: false,
              response,
            });
            requestMetricAttrs = apiRequestMetricAttrs(metricAttrs, {
              result: 'success',
              attemptCount,
              response,
            });
          }
          lc.debug?.('fetch from API server succeeded');
          return result;
        } catch (error) {
          lc.warn?.(
            'failed to parse response',
            {
              url: finalUrl,
            },
            error,
          );

          const errorBody = apiFailedBody(
            source,
            ErrorReason.Parse,
            `Failed to parse response from API server: ${getErrorMessage(error)}`,
          );
          recordApiAttempt(performance.now() - attemptStart, metricAttrs, {
            attempt,
            result: 'parse_error',
            willRetry: false,
            response,
            errorBody,
          });
          requestMetricAttrs = apiRequestMetricAttrs(metricAttrs, {
            result: 'parse_error',
            attemptCount,
            response,
            errorBody,
          });
          throw new ProtocolErrorWithLevel(errorBody, 'warn', {cause: error});
        }
      } catch (error) {
        if (isProtocolError(error)) {
          throw error;
        }

        const isFetchFailed =
          error instanceof TypeError && error.message === 'fetch failed';
        // unexpected/unknown errors should be logged at 'error' level so they
        // are investigated
        let logLevel: LogLevel = isFetchFailed ? 'warn' : 'error';
        lc[logLevel]?.(
          'fetch from API server threw error',
          {url: finalUrl},
          error,
        );

        const willRetry = isFetchFailed && attempt < MAX_ATTEMPTS;
        recordApiAttempt(performance.now() - attemptStart, metricAttrs, {
          attempt,
          result: 'fetch_error',
          willRetry,
        });

        if (willRetry && (await sleepBeforeRetry())) {
          continue;
        }

        const errorBody = apiFailedBody(
          source,
          ErrorReason.Internal,
          `Fetch from API server threw error: ${getErrorMessage(error)}`,
        );
        requestMetricAttrs = apiRequestMetricAttrs(metricAttrs, {
          result: 'fetch_error',
          attemptCount,
          errorBody,
        });
        throw new ProtocolErrorWithLevel(errorBody, logLevel, {cause: error});
      }
    }
    unreachable();
  } finally {
    const attrs =
      requestMetricAttrs ??
      apiRequestMetricAttrs(metricAttrs, {
        result: attemptCount === 0 ? 'config_error' : 'fetch_error',
        attemptCount,
      });
    apiRequests().add(1, attrs);
    apiRequestDuration().recordMs(performance.now() - requestStart, attrs);
    apiInFlight().add(-1, metricAttrs);
  }
}

/**
 * Returns true if the url matches one of the allowedUrlPatterns.
 *
 * URLPattern automatically ignores query parameters and hash fragments during matching
 * because it sets search and hash to wildcard ('*') by default.
 *
 * Example URLPattern patterns:
 * - "https://api.example.com/endpoint" - Exact match for a specific URL
 * - "https://*.example.com/endpoint" - Matches any single subdomain (e.g., "https://api.example.com/endpoint")
 * - "https://*.*.example.com/endpoint" - Matches two subdomains (e.g., "https://api.v1.example.com/endpoint")
 * - "https://api.example.com/*" - Matches any path under /
 * - "https://api.example.com/:version/endpoint" - Matches with named parameter (e.g., "https://api.example.com/v1/endpoint")
 */
export function urlMatch(
  url: string,
  allowedUrlPatterns: readonly URLPattern[],
): boolean {
  for (const pattern of allowedUrlPatterns) {
    if (pattern.test(url)) {
      return true;
    }
  }
  return false;
}

/**
 * Returns the delay in milliseconds for the next retry attempt using exponential backoff with jitter.
 *
 * The delay assumes the first retry is attempt 1.
 * The formula is: `min(1000, 100 * 2^(attempt - 1) + jitter)` where jitter is between 0 and 100ms.
 */
function getBackoffDelayMs(attempt: number): number {
  return Math.min(1000, 100 * Math.pow(2, attempt - 1) + Math.random() * 100);
}

function apiFailedBody(
  source: 'push' | 'transform',
  reason: ApiFailedReason,
  message: string,
  response?: Response | undefined,
  bodyPreview?: string | undefined,
): ErrorBody {
  if (source === 'push') {
    return reason === ErrorReason.HTTP
      ? {
          kind: ErrorKind.PushFailed,
          origin: ErrorOrigin.ZeroCache,
          reason,
          status: response?.status ?? 0,
          ...(bodyPreview !== undefined ? {bodyPreview} : {}),
          message,
          mutationIDs: [],
        }
      : {
          kind: ErrorKind.PushFailed,
          origin: ErrorOrigin.ZeroCache,
          reason,
          message,
          mutationIDs: [],
        };
  }

  return reason === ErrorReason.HTTP
    ? {
        kind: ErrorKind.TransformFailed,
        origin: ErrorOrigin.ZeroCache,
        reason,
        status: response?.status ?? 0,
        ...(bodyPreview !== undefined ? {bodyPreview} : {}),
        message,
        queryIDs: [],
      }
    : {
        kind: ErrorKind.TransformFailed,
        origin: ErrorOrigin.ZeroCache,
        reason,
        message,
        queryIDs: [],
      };
}

type ApiErrorMetricBody = {
  kind: ErrorBody['kind'];
  reason?: string | undefined;
};

function apiErrorFromResult(result: unknown): ApiErrorMetricBody | undefined {
  const parsed = errorBodySchema.try(result, {mode: 'passthrough'});
  if (parsed.ok) {
    return parsed.value;
  }

  if (Array.isArray(result) && result[0] === 'transformFailed') {
    const legacyTransformFailed = errorBodySchema.try(result[1], {
      mode: 'passthrough',
    });
    return legacyTransformFailed.ok ? legacyTransformFailed.value : undefined;
  }

  const legacyPushError = pushErrorSchema.try(result, {mode: 'passthrough'});
  return legacyPushError.ok
    ? {
        kind: ErrorKind.PushFailed,
        reason: legacyPushErrorReason(legacyPushError.value.error),
      }
    : undefined;
}

function legacyPushErrorReason(error: PushError['error']): string {
  switch (error) {
    case 'http':
      return ErrorReason.HTTP;
    case 'unsupportedPushVersion':
      return ErrorReason.UnsupportedPushVersion;
    case 'unsupportedSchemaVersion':
    case 'zeroPusher':
      return ErrorReason.Internal;
  }
}

type ApiResponseErrorMetricAttrs = Pick<
  ApiRequestMetricAttrs,
  'http_status_code' | 'http_status_class' | 'error_kind' | 'error_reason'
>;

type ApiRequestMetricAttrsOptions = {
  result: ApiRequestResult;
  attemptCount: number;
  response?: Response | undefined;
  errorBody?: ApiErrorMetricBody | undefined;
};

type ApiAttemptMetricAttrsOptions = {
  attempt: number;
  result: ApiAttemptResult;
  willRetry: boolean;
  response?: Response | undefined;
  errorBody?: ApiErrorMetricBody | undefined;
};

function apiRequestMetricAttrs(
  baseAttrs: ApiMetricBaseAttrs,
  {result, attemptCount, response, errorBody}: ApiRequestMetricAttrsOptions,
): ApiRequestMetricAttrs {
  return {
    ...baseAttrs,
    result,
    attempt_count: attemptCount,
    ...apiResponseErrorMetricAttrs(response, errorBody),
  };
}

function apiResponseErrorMetricAttrs(
  response: Response | undefined,
  errorBody: ApiErrorMetricBody | undefined,
): ApiResponseErrorMetricAttrs {
  const attrs: ApiResponseErrorMetricAttrs = {};

  if (response) {
    attrs.http_status_code = response.status;
    attrs.http_status_class = `${Math.floor(response.status / 100)}xx`;
  }

  if (errorBody) {
    attrs.error_kind = errorBody.kind;
    if (errorBody.reason !== undefined) {
      attrs.error_reason = errorBody.reason;
    }
  }

  return attrs;
}

function recordApiAttempt(
  durationMs: number,
  baseAttrs: ApiMetricBaseAttrs,
  {
    attempt,
    result,
    willRetry,
    response,
    errorBody,
  }: ApiAttemptMetricAttrsOptions,
) {
  const attrs: ApiAttemptMetricAttrs = {
    ...baseAttrs,
    attempt,
    result,
    will_retry: willRetry,
    ...apiResponseErrorMetricAttrs(response, errorBody),
  };
  apiAttempts().add(1, attrs);
  apiAttemptDuration().recordMs(durationMs, attrs);
}
