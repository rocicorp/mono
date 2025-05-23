import {readdirSync} from 'node:fs';
import {defineConfig} from 'vitest/config';

const special = ['shared', 'zero-cache', 'z2s', 'zero-pg'];

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
    workspace: [...getPackages(), 'apps/zbugs/vitest.config.*.ts','tools/*'],
  },
});
