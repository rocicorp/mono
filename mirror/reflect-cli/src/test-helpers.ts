import {afterEach, beforeEach} from '@jest/globals';
import sign from 'jwt-encode';
import {setAuthConfigForTesting} from './auth-config.js';

export function useFakeAuthConfig() {
  const secret = 'fake-secret';
  const idToken = {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    user_id: 'fake-uid',
    // This is for testing... add more as needed
  };
  const expirationTime = 1234567890;
  const newConfig = {
    idToken: sign(idToken, secret),
    expirationTime,
    refreshToken: 'fake-refresh-token',
  };

  beforeEach(() => {
    setAuthConfigForTesting(newConfig);
  });

  afterEach(() => {
    setAuthConfigForTesting(undefined);
  });
}
