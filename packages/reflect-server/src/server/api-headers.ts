export const API_KEY_HEADER_NAME = 'x-reflect-api-key';

export function createAPIHeaders(authApiKey: string) {
  const headers = new Headers();
  headers.set(API_KEY_HEADER_NAME, authApiKey);
  return headers;
}
