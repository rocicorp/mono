import {defineWorkspace} from 'vitest/config';

export default defineWorkspace([
  './mirror/cloudflare-api/vitest.config.ts',
  './packages/zql/vitest.config.ts',
  './packages/reflect-react/vitest.config.ts',
  './packages/reflect-client/vitest.config.ts',
  './packages/shared/vitest.config.ts',
  './packages/reflect-shared/vitest.config.ts',
  './packages/zero-client/vitest.config.ts',
]);
