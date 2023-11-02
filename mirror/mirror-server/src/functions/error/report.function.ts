import {FunctionsErrorCode, HttpsError} from 'firebase-functions/v2/https';
import {
  errorReportingRequestSchema,
  errorReportingResponseSchema,
} from 'mirror-protocol/src/error.js';

import {logger} from 'firebase-functions';
import {validateSchema} from '../validators/schema.js';

const rociTeamUserID: {[id: string]: boolean} = {
  ['Sr0S3VOyqIN9O06nMACgwqQ3GvK2']: true,
  ['fH958LU8qyMmfVrfTzE0egkxsHk1']: true,
  ['IrSbtZGlYGfLKYpiwYLiISdZ7Hl2']: true,
  ['hu0ggohMptVpC4GRn6GhfN9dhcO2']: true,
  ['Dplw09NbNaWMVFLAoKYlTbJXuha2']: true,
  ['02Yam8WlcQfC3lf7Rf803e9eoWp2']: true,
} as const;

export const report = () =>
  validateSchema(
    errorReportingRequestSchema,
    errorReportingResponseSchema,
  ).handle((request, _context) => {
    const {
      severity,
      action,
      error: {desc, name, stack},
      requester: {userID},
    } = request;

    // Default: 5xx error code.
    let errorCode: FunctionsErrorCode = 'unknown';

    // Choose 4xx error codes for conditions that should only
    // alert at a higher threshold.
    if (severity === 'WARNING') {
      errorCode = 'cancelled';
    } else if (name === 'FirebaseError') {
      // Server-returned error has presumably already been reported.
      errorCode = 'already-exists';
    } else if (rociTeamUserID[userID]) {
      errorCode = 'aborted';
    } else if (shouldBeWarning(action, desc)) {
      // These should eventually go away as users update their reflect-cli versions.
      logger.warn(`Reclassifying error as warning`, request);
      errorCode = 'cancelled';
    }

    // 4xx and 5xx errors have different alerting thresholds.
    // "cancelled" maps to 499 and "unknown" maps to 500
    const error = new HttpsError(
      errorCode,
      `action: ${action}, description: ${desc}`,
    );
    if (stack) {
      // Inherit the stack from the reported error so that errors are easier to
      // bucket (both manually and by the Error Reporter).
      error.stack = `action: ${action}, description: ${stack}`;
    }
    throw error;
  });

function shouldBeWarning(action: string, desc: string): boolean {
  if (action === 'cmd_dev') {
    // https://github.com/rocicorp/mono/issues/1126
    if (
      desc.startsWith('Error: ENOENT: no such file or directory') ||
      desc.startsWith(
        'MiniflareCoreError [ERR_RUNTIME_FAILURE]: The Workers runtime failed to start.',
      )
    ) {
      return true;
    }
  }
  if (desc.startsWith('Error: Command failed: npm ')) {
    // https://github.com/rocicorp/mono/issues/1129
    return true;
  }
  if (action === 'cmd_publish') {
    // https://github.com/rocicorp/mono/issues/1152
    if (desc.startsWith('Error: Build failed')) {
      return true;
    }
  }
  if (desc.startsWith('Error: Login did not complete within 2 minutes')) {
    // Downgraded to warning by cli: https://github.com/rocicorp/mono/pull/1187
    return true;
  }
  return false;
}
