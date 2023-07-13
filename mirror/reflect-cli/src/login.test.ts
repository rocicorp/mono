import {expect, test, describe} from '@jest/globals';
import {loginHandler} from './login.js';
import {mockHttpServer} from './login.test.helper.js';
import type http from 'node:http';
import type {UserAuthConfig} from './auth-config.js';

const credentialReceiverServerFetch: (
  req: Request,
) => Promise<http.ServerResponse<http.IncomingMessage>> = mockHttpServer();

describe('loginHandler', () => {
  test('should reject if customToken, refreshToken or expirationTime is missing', async () => {
    const callbackUrl = new URL('http://localhost:8976/oauth/callback');
    let openInBrowserCalled = false;
    let writeAuthConfigFileCalled = false;

    const loginHandlerPromise = loginHandler(
      async url => {
        openInBrowserCalled = true;
        expect(url).toEqual('https://reflect.net/auth');
        const serverResponse = await credentialReceiverServerFetch(
          new Request(callbackUrl.toString()),
        );
        expect(serverResponse).toBeDefined();
      },
      (config: UserAuthConfig) => {
        expect(config).toBeDefined();
        expect(config.customToken).toEqual('valid-token');
        writeAuthConfigFileCalled = true;
      },
    );

    await expect(loginHandlerPromise).rejects.toThrow(
      'Error saving credentials: Error: Missing customToken from the auth provider.',
    );
    expect(openInBrowserCalled).toEqual(true);
    expect(writeAuthConfigFileCalled).toEqual(false);
  });

  test('should pass if customToken is valid', async () => {
    // spyOn writeAuthConfigFile
    const callbackUrl = new URL('http://localhost:8976/oauth/callback');
    callbackUrl.searchParams.set('customToken', 'valid-token');
    let openInBrowserCalled = false;
    let writeAuthConfigFileCalled = false;

    const loginHandlerPromise = loginHandler(
      async url => {
        openInBrowserCalled = true;
        expect(url).toEqual('https://reflect.net/auth');
        const serverResponse = await credentialReceiverServerFetch(
          new Request(callbackUrl.toString()),
        );
        expect(serverResponse).toBeDefined();
      },
      (config: UserAuthConfig) => {
        expect(config).toBeDefined();
        expect(config.customToken).toEqual('valid-token');
        writeAuthConfigFileCalled = true;
      },
    );

    await expect(loginHandlerPromise).resolves.toBeUndefined();
    expect(openInBrowserCalled).toEqual(true);
    expect(writeAuthConfigFileCalled).toEqual(true);
  });
});
