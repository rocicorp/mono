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
    const apiResponse = makeAPIError(this.#info);
    return new Response(JSON.stringify(apiResponse), {status: this.#info.code});
  }
}

export function roomNotFoundAPIError(roomID: string): APIError {
  return new APIError(404, 'rooms', `Room "${roomID}" not found`);
}
