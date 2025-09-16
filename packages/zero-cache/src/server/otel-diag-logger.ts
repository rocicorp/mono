/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {diag, DiagLogLevel} from '@opentelemetry/api';
import {LogContext} from '@rocicorp/logger';

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
        // Check if this is a known non-critical error that should be a warning
        if (
          msg.includes('Request Timeout') ||
          msg.includes('Unexpected server response: 502') ||
          msg.includes('Export failed with retryable status') ||
          msg.includes('Method Not Allowed') ||
          msg.includes('socket hang up')
        ) {
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
