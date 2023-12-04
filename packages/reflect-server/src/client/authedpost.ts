import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {createAPIHeaders} from '../server/api-headers.js';

export function newAuthedPostRequest(
  url: URL,
  authApiKey: string,
  req?: ReadonlyJSONValue | undefined,
) {
  return new Request(url.toString(), {
    method: 'POST',
    headers: createAPIHeaders(authApiKey),
    body: req !== undefined ? JSON.stringify(req) : null,
  });
}
