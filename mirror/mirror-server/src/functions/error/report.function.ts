import {FunctionsErrorCode, HttpsError} from 'firebase-functions/v2/https';
import {
  errorReportingRequestSchema,
  errorReportingResponseSchema,
} from 'mirror-protocol/src/error.js';

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
      error: {desc, name},
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
    }

    // 4xx and 5xx errors have different alerting thresholds.
    // "cancelled" maps to 499 and "unknown" maps to 500
    throw new HttpsError(errorCode, `action: ${action}, description: ${desc}`);
  });
