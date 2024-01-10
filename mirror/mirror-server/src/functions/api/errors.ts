import {HttpsError} from 'firebase-functions/v2/https';

export function unsupportedMethodError(
  msg: string = 'Unsupported method',
): HttpsError {
  const error = new HttpsError('invalid-argument', msg);
  // There's no FunctionsErrorCode for 405: Unsupported Method, so we hack it.
  error.httpErrorCode.status = 405;
  return error;
}
