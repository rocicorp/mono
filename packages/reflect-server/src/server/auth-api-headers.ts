export const AUTH_API_KEY_HEADER_NAME = 'x-reflect-auth-api-key';

export function createAuthAPIHeaders(authApiKey: string) {
  const headers = new Headers();
  headers.set(AUTH_API_KEY_HEADER_NAME, authApiKey);
  return headers;
}
