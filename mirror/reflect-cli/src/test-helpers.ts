import {afterEach, beforeEach} from '@jest/globals';
import {setAuthConfigForTesting} from './auth-config.js';

export function useFakeAuthConfig() {
  const newConfig = {
    customToken: 'fake-custom-token',
  };

  beforeEach(() => {
    setAuthConfigForTesting(newConfig);
  });

  afterEach(() => {
    setAuthConfigForTesting(undefined);
  });
}
