import {expect, test, describe} from '@jest/globals';
import {loginHandler} from './login.js';
import {mockHttpServer} from './login.test.helper.js';
import type http from 'node:http';
import type {UserAuthConfig} from './auth-config.js';

const credentialReceiverServerFetch: (
  req: Request,
) => Promise<http.ServerResponse<http.IncomingMessage>> = mockHttpServer();

describe('loginHandler', () => {
  test('should reject if idToken, refreshToken or expirationTime is missing', async () => {
    const callbackUrl = new URL('http://localhost:8976/oauth/callback');
    callbackUrl.searchParams.set('idToken', 'valid-token');
    callbackUrl.searchParams.set('refreshToken', 'valid-refresh-token');
    let openInBrowserCalled = false;
    let writeAuthConfigFileCalled = false;

    const loginHandlerPromise = loginHandler(
      async url => {
        openInBrowserCalled = true;
        expect(url).toEqual('https://auth.reflect.net');
        const serverResponse = await credentialReceiverServerFetch(
          new Request(callbackUrl.toString()),
        );
        expect(serverResponse).toBeDefined();
      },
      (config: UserAuthConfig) => {
        expect(config).toBeDefined();
        expect(config.idToken).toEqual('valid-token');
        expect(config.refreshToken).toEqual('valid-refresh-token');
        writeAuthConfigFileCalled = true;
      },
    );

    await expect(loginHandlerPromise).rejects.toThrow(
      'Error saving credentials: Error: Missing expirationTime from the auth provider.',
    );
    expect(openInBrowserCalled).toEqual(true);
    expect(writeAuthConfigFileCalled).toEqual(false);
  });

  test('should pass if idToken, refreshToken or expirationTime are valid', async () => {
    // spyOn writeAuthConfigFile
    const callbackUrl = new URL('http://localhost:8976/oauth/callback');
    callbackUrl.searchParams.set('idToken', 'valid-token');
    callbackUrl.searchParams.set('refreshToken', 'valid-refresh-token');
    callbackUrl.searchParams.set('expirationTime', '0');
    let openInBrowserCalled = false;
    let writeAuthConfigFileCalled = false;

    const loginHandlerPromise = loginHandler(
      async url => {
        openInBrowserCalled = true;
        expect(url).toEqual('https://auth.reflect.net');
        const serverResponse = await credentialReceiverServerFetch(
          new Request(callbackUrl.toString()),
        );
        expect(serverResponse).toBeDefined();
      },
      (config: UserAuthConfig) => {
        expect(config).toBeDefined();
        expect(config.idToken).toEqual('valid-token');
        expect(config.refreshToken).toEqual('valid-refresh-token');
        expect(config.expirationTime).toEqual(0);
        writeAuthConfigFileCalled = true;
      },
    );

    await expect(loginHandlerPromise).resolves.toBeUndefined();
    expect(openInBrowserCalled).toEqual(true);
    expect(writeAuthConfigFileCalled).toEqual(true);
  });

  test('should reject if expirationTime is not a number', async () => {
    const callbackUrl = new URL('http://localhost:8976/oauth/callback');
    callbackUrl.searchParams.set('idToken', 'valid-token');
    callbackUrl.searchParams.set('refreshToken', 'valid-refresh-token');
    callbackUrl.searchParams.set('expirationTime', 'invalid-expiration-time');
    let openInBrowserCalled = false;
    let writeAuthConfigFileCalled = false;
    const loginHandlerPromise = loginHandler(
      async url => {
        openInBrowserCalled = true;
        expect(url).toEqual('https://auth.reflect.net');
        const serverResponse = await credentialReceiverServerFetch(
          new Request(callbackUrl.toString()),
        );
        expect(serverResponse).toBeDefined();
      },
      (_config: UserAuthConfig) => {
        writeAuthConfigFileCalled = true;
      },
    );

    await expect(loginHandlerPromise).rejects.toThrow(
      'Error saving credentials: AssertionError [ERR_ASSERTION]: expirationTime is not a number',
    );
    expect(openInBrowserCalled).toEqual(true);
    expect(writeAuthConfigFileCalled).toEqual(false);
  });
});
