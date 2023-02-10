import {assertNumber, assertObject, assertString} from './asserts.js';

export function isHTTPRequestInfo(
  v: Record<string, unknown>,
): v is HTTPRequestInfo {
  return (
    typeof v.httpStatusCode === 'number' && typeof v.errorMessage === 'string'
  );
}

export function assertHTTPRequestInfo(
  v: unknown,
): asserts v is HTTPRequestInfo {
  assertObject(v);
  assertNumber(v.httpStatusCode);
  assertString(v.errorMessage);
}

export type HTTPRequestInfo = {
  httpStatusCode: number;
  errorMessage: string;
};
