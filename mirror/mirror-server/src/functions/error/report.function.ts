import {
  errorReportingRequestSchema,
  errorReportingResponseSchema,
} from 'mirror-protocol/src/error.js';
import {HttpsError} from 'firebase-functions/v2/https';

import {validateSchema} from '../validators/schema.js';

export const report = () =>
  validateSchema(
    errorReportingRequestSchema,
    errorReportingResponseSchema,
  ).handle((request, _context) => {
    const {
      severity,
      action,
      error: {desc},
    } = request;

    // 4xx and 5xx errors have different alerting thresholds.
    // "cancelled" maps to 499 and "unknown" maps to 500
    throw new HttpsError(
      severity === 'WARNING' ? 'cancelled' : 'unknown',
      `action: ${action}, description: ${desc}`,
    );
  });
