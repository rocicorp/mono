import type {ReadonlyJSONValue} from 'replicache';
import {createAuthAPIHeaders} from '../server/auth-api-headers.js';

export function newAuthedPostRequest(
  url: URL,
  authApiKey: string,
  req?: ReadonlyJSONValue | undefined,
) {
  return new Request(url.toString(), {
    method: 'POST',
    headers: createAuthAPIHeaders(authApiKey),
    body: req !== undefined ? JSON.stringify(req) : null,
  });
}
