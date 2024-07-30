import {defineWorkspace} from 'vitest/config';

export default defineWorkspace([
  './packages/zql/vitest.config.ts',
  './packages/shared/vitest.config.ts',
  './packages/zero-client/vitest.config.ts',
]);
