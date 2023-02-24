import type {RelaxedJSONValue} from 'protocol/src/json.js';
import {createAuthAPIHeaders} from '../server/auth-api-headers.js';

export function newAuthedPostRequest(
  url: URL,
  authApiKey: string,
  req?: RelaxedJSONValue | undefined,
) {
  return new Request(url.toString(), {
    method: 'POST',
    headers: createAuthAPIHeaders(authApiKey),
    body: req !== undefined ? JSON.stringify(req) : null,
  });
}
