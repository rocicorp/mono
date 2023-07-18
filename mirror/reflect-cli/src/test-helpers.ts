import {afterEach, beforeEach} from '@jest/globals';
import {setAuthConfigForTesting} from './auth-config.js';

export function useFakeAuthConfig() {
  const newConfig = {
    authCredential: {
      accessToken: 'valid-token',
      providerId: 'github.com',
      signInMethod: 'github.com',
    },
  };

  beforeEach(() => {
    setAuthConfigForTesting(newConfig);
  });

  afterEach(() => {
    setAuthConfigForTesting(undefined);
  });
}
