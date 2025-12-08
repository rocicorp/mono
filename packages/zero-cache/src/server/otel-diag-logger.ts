import {diag, DiagLogLevel} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';

function getOtelLogLevel(level: string | undefined): DiagLogLevel | undefined {
  if (!level) return undefined;

  const normalizedLevel = level.toLowerCase();
  switch (normalizedLevel) {
    case 'none':
      return DiagLogLevel.NONE;
    case 'error':
      return DiagLogLevel.ERROR;
    case 'warn':
    case 'warning':
      return DiagLogLevel.WARN;
    case 'info':
      return DiagLogLevel.INFO;
    case 'debug':
      return DiagLogLevel.DEBUG;
    case 'verbose':
      return DiagLogLevel.VERBOSE;
    case 'all':
      return DiagLogLevel.ALL;
    default:
      return undefined;
  }
}

let diagLoggerConfigured = false;

/**
 * Sets up the OpenTelemetry diagnostic logger with custom error handling and suppression.
 * This function can be called multiple times safely - it will only configure the logger once per LogContext.
 *
 * @param lc LogContext for routing OTEL diagnostic messages to the application logger
 * @param force If true, will reconfigure even if already configured (useful after NodeSDK setup)
 * @returns true if the logger was configured, false if it was already configured and not forced
 */
export function setupOtelDiagnosticLogger(
  lc?: LogContext,
  force = false,
): boolean {
  if (!lc) {
    return false;
  }

  if (!force && diagLoggerConfigured) {
    return false;
  }

  const log = lc.withContext('component', 'otel');
  diag.setLogger(
    {
      verbose: (msg: string, ...args: unknown[]) => log.debug?.(msg, ...args),
      debug: (msg: string, ...args: unknown[]) => log.debug?.(msg, ...args),
      info: (msg: string, ...args: unknown[]) => log.info?.(msg, ...args),
      warn: (msg: string, ...args: unknown[]) => log.warn?.(msg, ...args),
      error: (msg: string, ...args: unknown[]) => {
        if (shouldWarnForOtelError(msg)) {
          log.warn?.(msg, ...args);
        } else {
          log.error?.(msg, ...args);
        }
      },
    },
    {
      logLevel:
        getOtelLogLevel(process.env.OTEL_LOG_LEVEL) ?? DiagLogLevel.ERROR,
      suppressOverrideMessage: true,
    },
  );

  diagLoggerConfigured = true;
  return true;
}

/**
 * Reset the diagnostic logger configuration state.
 * This is primarily useful for testing scenarios.
 */
export function resetOtelDiagnosticLogger(): void {
  diagLoggerConfigured = false;
}

const NON_CRITICAL_OTEL_ERRORS = [
  'request timeout',
  'unexpected server response: 502',
  'export failed with retryable status',
  'export took longer than',
  'metrics export failed',
  'method not allowed',
  'socket hang up',
  'payment required',
];

function shouldWarnForOtelError(msg: string): boolean {
  const errorMessage = String(msg ?? '').toLowerCase();
  return NON_CRITICAL_OTEL_ERRORS.some(pattern =>
    errorMessage.includes(pattern),
  );
}
