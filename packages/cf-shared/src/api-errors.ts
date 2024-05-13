import {
  makeAPIError,
  type APIErrorCode,
  type APIErrorInfo,
} from 'shared/src/api/responses.js';
import {ErrorWithResponse} from './errors.js';

export class APIError extends ErrorWithResponse {
  readonly #info: APIErrorInfo;

  constructor(code: APIErrorCode, resource: string, message: string) {
    super(`${code}: ${message}${resource ? ' (' + resource + ')' : ''}`);
    this.#info = {code, resource, message};
  }

  response(): Response {
    return makeAPIErrorResponse(this.#info);
  }
}

export function makeAPIErrorResponse(info: APIErrorInfo): Response {
  const apiResponse = makeAPIError(info);
  return new Response(JSON.stringify(apiResponse), {
    status: info.code,
    headers: {'Content-Type': 'application/json'},
  });
}

export function roomNotFoundAPIError(roomID: string): APIError {
  return new APIError(404, 'rooms', `Room "${roomID}" not found`);
}
