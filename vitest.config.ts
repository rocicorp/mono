import {readdirSync} from 'node:fs';
import {defineConfig} from 'vitest/config';

const special = [
  'shared',
  'replicache',
  'zero-cache',
  'z2s',
  'zero-pg',
  'zero-server',
  'zql-integration-tests',
];

// Get all the dirs in packages
function getPackages() {
  return readdirSync(new URL('packages', import.meta.url), {
    withFileTypes: true,
  })
    .filter(f => f.isDirectory())
    .map(
      f =>
        `packages/${special.includes(f.name) ? `${f.name}/vitest.config.*.ts` : f.name}`,
    );
}

export default defineConfig({
  test: {
    projects: [...getPackages(), 'apps/zbugs/vitest.config.*.ts', 'tools/*'],
  },
});
