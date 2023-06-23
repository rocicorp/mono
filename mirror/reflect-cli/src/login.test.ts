import {expect, test, describe} from '@jest/globals';
import {loginHandler} from './login.js';
import {mockHttpServer} from './login.test.helper.js';
import type http from 'node:http';

const credentialReceiverServerFetch: (
  req: Request,
) => Promise<http.ServerResponse<http.IncomingMessage>> = mockHttpServer();

describe('loginHandler', () => {
  test('should reject if idToken, refreshToken or expirationTime is missing', async () => {
    const callbackUrl = new URL('http://localhost:8976/oauth/callback');
    callbackUrl.searchParams.set('idToken', 'valid-token');
    callbackUrl.searchParams.set('refreshToken', 'valid-refresh-token');
    // callbackUrl.searchParams.set('expirationTime', 'invalid-expir0ation-time');
    let openInBrowserCalled = false;
    const loginHandlerPromise = loginHandler(async url => {
      openInBrowserCalled = true;
      expect(url).toEqual('https://auth.reflect.net');
      const serverResponse = await credentialReceiverServerFetch(
        new Request(callbackUrl.toString()),
      );
      expect(serverResponse).toBeDefined();
    });

    await expect(loginHandlerPromise).rejects.toThrow(
      'Invalid idToken, refreshToken, or expiresIn from the auth provider.',
    );
    expect(openInBrowserCalled).toEqual(true);
  });
});
