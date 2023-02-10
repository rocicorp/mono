export function createAuthAPIHeaders(authApiKey: string) {
  const headers = new Headers();
  headers.set('x-reflect-auth-api-key', authApiKey);
  return headers;
}
