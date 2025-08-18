import {readdirSync} from 'node:fs';
import {defineConfig} from 'vitest/config';

const special = ['shared', 'replicache', 'z2s', 'zero-pg', 'zero-server'];

// Get all the dirs in packages
function getPackages() {
  return readdirSync(new URL('packages', import.meta.url), {
    withFileTypes: true,
  })
    .filter(
      f =>
        f.isDirectory() &&
        f.name !== 'zero-cache' &&
        f.name !== 'zql-integration-tests',
    )
    .map(
      f =>
        `packages/${special.includes(f.name) ? `${f.name}/vitest.config.*.ts` : f.name}`,
    );
}

const projects = [
  ...getPackages(),
  'apps/zbugs/vitest.config.*.ts',
  'apps/otel-proxy/vitest.config.ts',
  'tools/*',

  'packages/zero-cache/vitest.config.no-pg.ts',
  // Running 15, 16 and 17 breaks change-streamer tests
  // 'packages/zero-cache/vitest.config.pg-15.ts',
  // 'packages/zero-cache/vitest.config.pg-16.ts',
  'packages/zero-cache/vitest.config.pg-17.ts',

  // Running 15, 16 and 17 breaks change-streamer tests
  // 'packages/zql-integration-tests/vitest.config.pg-15.ts',
  // 'packages/zql-integration-tests/vitest.config.pg-16.ts',
  'packages/zql-integration-tests/vitest.config.pg-17.ts',
];

export default defineConfig({
  test: {
    projects,
  },
});
