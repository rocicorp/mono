import {expect, test, describe} from '@jest/globals';
import {loginHandler} from './login.js';
import {mockHttpServer} from './login.test.helper.js';
import type http from 'node:http';
import type {UserAuthConfig} from './auth-config.js';

const credentialReceiverServerFetch: (
  req: Request,
) => Promise<http.ServerResponse<http.IncomingMessage>> = mockHttpServer();

describe('loginHandler', () => {
  test('should reject if authCredential is missing', async () => {
    const callbackUrl = new URL('http://localhost:8976/oauth/callback');
    let openInBrowserCalled = false;
    let writeAuthConfigFileCalled = false;

    const loginHandlerPromise = loginHandler(
      false,
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
        expect(config.authCredential).toEqual({
          accessToken: 'valid-token',
          signInMethod: 'github.com',
          providerId: 'github.com',
        });
        writeAuthConfigFileCalled = true;
      },
    );

    await expect(loginHandlerPromise).rejects.toThrow(
      'Error saving credentials: Error: Missing auth credential from the auth provider.',
    );
    expect(openInBrowserCalled).toEqual(true);
    expect(writeAuthConfigFileCalled).toEqual(false);
  });

  test('should pass if authCredential is valid', async () => {
    // spyOn writeAuthConfigFile
    const callbackUrl = new URL('http://localhost:8976/oauth/callback');
    callbackUrl.searchParams.set(
      'authCredential',
      '{"accessToken":"valid-token","providerId":"github.com","signInMethod":"github.com"}',
    );
    let openInBrowserCalled = false;
    let writeAuthConfigFileCalled = false;

    const loginHandlerPromise = loginHandler(
      false,
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
        expect(config.authCredential).toEqual({
          accessToken: 'valid-token',
          signInMethod: 'github.com',
          providerId: 'github.com',
        });
        writeAuthConfigFileCalled = true;
      },
    );

    await expect(loginHandlerPromise).resolves.toBeUndefined();
    expect(openInBrowserCalled).toEqual(true);
    expect(writeAuthConfigFileCalled).toEqual(true);
  });
});
