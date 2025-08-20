import {readdirSync} from 'node:fs';
import {defineConfig} from 'vitest/config';

const {TEST_PG_MODE} = process.env;

// Find all vitest.config*.ts files up to depth 2 from repo root, skipping node_modules.
function* getProjects(): Iterable<string> {
  const maxDepth = 2; // depth relative to repo root

  function* walk(
    basePath: string,
    dirUrl: URL,
    depthLeft: number,
  ): Generator<string> {
    const entries = readdirSync(dirUrl, {withFileTypes: true});
    for (const e of entries) {
      if (e.isDirectory()) {
        if (e.name === 'node_modules') continue;
        if (depthLeft > 0) {
          yield* walk(
            `${basePath}${e.name}/`,
            new URL(`${e.name}/`, dirUrl),
            depthLeft - 1,
          );
        }
      } else if (e.isFile()) {
        if (/^vitest\.config.*\.ts$/.test(e.name)) {
          const rel = `${basePath}${e.name}`;
          // Avoid referencing this root config file itself
          if (rel !== 'vitest.config.ts') {
            yield rel;
          }
        }
      }
    }
  }

  yield* walk('', new URL('./', import.meta.url), maxDepth);
}

function filterTestName(name: string) {
  if (TEST_PG_MODE === 'nopg') {
    return !name.includes('pg-');
  }
  if (TEST_PG_MODE === 'pg') {
    return name.includes('pg-');
  }
  return true;
}

const projects = [...getProjects()].filter(filterTestName);

export default defineConfig({
  test: {
    projects,
  },
});
