import {
  errorReportingRequestSchema,
  errorReportingResponseSchema,
} from 'mirror-protocol/src/app.js';
import {HttpsError} from 'firebase-functions/v2/https';

import {validateSchema} from '../validators/schema.js';

export const errorReporting = () =>
  validateSchema(errorReportingRequestSchema, errorReportingResponseSchema)
    .handle( (request, _context) => {
      throw new HttpsError(
        'unknown',
       request.errorMessage,
      );      
    });
