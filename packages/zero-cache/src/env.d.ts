import {DurableObjectNamespace} from '@cloudflare/workers-types';

// https://developers.cloudflare.com/workers/testing/vitest-integration/get-started/migrate-from-miniflare-2/#access-bindings
declare module 'cloudflare:test' {
  interface ProvidedEnv {
    runnerDO: DurableObjectNamespace;
  }
}
