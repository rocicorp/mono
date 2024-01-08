import type {ReadonlyJSONValue} from 'shared/src/json.js';

export type APIErrorCode = 400 | 404 | 405 | 409; // Add more as necessary.
export type APIResource = 'request' | 'rooms'; // Add more as necessary.

export type APIErrorInfo = {
  code: APIErrorCode;
  resource: string;
  message: string;
};

export type APIResponse<T extends ReadonlyJSONValue> =
  | {
      result: T;
      error: null;
    }
  | {
      result: null;
      error: APIErrorInfo;
    };

export function makeAPIResponse<T extends ReadonlyJSONValue>(
  result: T,
): APIResponse<T> {
  return {result, error: null};
}

export function makeAPIError(error: APIErrorInfo): APIResponse<null> {
  return {result: null, error};
}
