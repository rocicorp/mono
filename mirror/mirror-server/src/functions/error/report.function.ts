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
    throw new HttpsError(
      'unknown',
      `action: ${request.action}, description: ${request.error.desc}`,
    );
  });
