import type {JSONType} from '../protocol/json.js';
import {createAuthAPIHeaders} from '../server/auth-api-headers.js';

export function newAuthedPostRequest(
  url: URL,
  authApiKey: string,
  req?: JSONType | undefined,
) {
  return new Request(url.toString(), {
    method: 'POST',
    headers: createAuthAPIHeaders(authApiKey),
    body: req ? JSON.stringify(req) : undefined,
  });
}
