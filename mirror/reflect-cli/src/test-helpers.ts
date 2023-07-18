import {afterEach, beforeEach} from '@jest/globals';
import {setAppConfigForTesting, type AppConfig} from './app-config.js';
import {UserAuthConfig, setAuthConfigForTesting} from './auth-config.js';

export function useFakeAuthConfig() {
  const newConfig: UserAuthConfig = {
    authCredential: {
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

export function useFakeAppConfig() {
  const appConfig: AppConfig = {
    appID: 'test-app-id',
  };

  beforeEach(() => {
    setAppConfigForTesting(appConfig);
  });

  afterEach(() => {
    setAppConfigForTesting(undefined);
  });
}
