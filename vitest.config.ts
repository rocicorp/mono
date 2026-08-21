import {readdirSync} from 'node:fs';
import {defineConfig} from 'vitest/config';

// Electron-hosted terminals (VSCode's integrated terminal, Cursor, Claude
// Code, ...) leak ELECTRON_RUN_AS_NODE=1 into spawned shells. node-gyp-build
// treats the mere *presence* of that var as proof the process is Electron
// (see node-gyp-build.js's isElectron(), which checks
// `process.env.ELECTRON_RUN_AS_NODE` directly, without also requiring
// `process.versions.electron` to be set) — so it looks for an Electron-ABI
// prebuild of native deps like @rocicorp/zero-sqlite3, which isn't
// installed, and fails with a confusing "No native build was found" error
// across every test that touches SQLite. This must run before Vitest spawns
// its worker pool (workers inherit process.env at fork time), and doesn't
// affect genuine Electron test runs, which are still detected via the
// (unmodified) process.versions.electron check.
delete process.env.ELECTRON_RUN_AS_NODE;

const {TEST_PG_MODE} = process.env;

// Find all vitest.config*.ts files up to depth 2 from repo root, skipping node_modules.
function* getProjects(): Iterable<string> {
  const maxDepth = 2; // depth relative to repo root

  function* walk(
    basePath: string,
    dirUrl: URL,
    depth: number,
  ): Generator<string> {
    const entries = readdirSync(dirUrl, {withFileTypes: true});

    // Process files in this directory first
    const fileNames = entries.filter(e => e.isFile()).map(e => e.name);
    const configNames = fileNames.filter(name =>
      /^vitest\.config.*\.ts$/.test(name),
    );
    const hasSuffixed = configNames.some(name =>
      /^vitest\.config\.[^.]+\.ts$/.test(name),
    );

    for (const name of configNames) {
      // Skip the root config file to avoid self-reference
      if (basePath === '' && name === 'vitest.config.ts') continue;
      // If any suffixed config exists in this dir, exclude the base config
      if (name === 'vitest.config.ts' && hasSuffixed) continue;
      // Skip bench configs — those are run separately via `pnpm run bench`
      if (name.includes('.bench')) continue;
      yield `${basePath}${name}`;
    }

    // Recurse into subdirectories up to max depth, skipping node_modules
    for (const e of entries) {
      if (!e.isDirectory()) continue;
      if (e.name === 'node_modules') continue;
      // Skip tsnapi — run separately via `pnpm run api-snapshot`
      if (e.name === 'tsnapi') continue;
      if (depth > 0) {
        yield* walk(
          `${basePath}${e.name}/`,
          new URL(`${e.name}/`, dirUrl),
          depth - 1,
        );
      }
    }
  }

  yield* walk('', new URL('./', import.meta.url), maxDepth);
}

function filterTestName(name: string) {
  if (!TEST_PG_MODE) {
    return true;
  }
  if (TEST_PG_MODE === 'nopg') {
    return !name.includes('pg-');
  }
  if (TEST_PG_MODE === 'pg') {
    return name.includes('pg-');
  }
  return name.includes(TEST_PG_MODE);
}

const projects = [...getProjects()].filter(filterTestName);

export default defineConfig({
  test: {
    projects,
  },
});
