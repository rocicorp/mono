import {readdirSync} from 'node:fs';
import {defineConfig} from 'vitest/config';

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
      yield `${basePath}${name}`;
    }

    // Recurse into subdirectories up to max depth, skipping node_modules
    for (const e of entries) {
      if (!e.isDirectory()) continue;
      if (e.name === 'node_modules') continue;
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
