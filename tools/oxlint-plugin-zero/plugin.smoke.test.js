import {spawnSync} from 'node:child_process';
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {afterEach, expect, test} from 'vitest';

/**
 * Smoke test for no-select-star as the linter runs it, not for its predicates.
 *
 * The unit tests next door cover the predicates directly. They cannot catch the
 * failure that actually happened: no-select-star's include prefixes never
 * matched, so the rule quietly did not run and the only symptom was an
 * "unused disable directive" elsewhere. A rule that cannot report is
 * indistinguishable from a clean codebase, so the rule is exercised end to end
 * against known-bad input.
 */

const HERE = dirname(fileURLToPath(import.meta.url));
const PLUGIN = join(HERE, 'index.js');

/**
 * Where oxlint's CLI actually lives, resolved through its manifest.
 *
 * Not a hardcoded hop into the root node_modules: that assumes a hoisted layout,
 * which a filtered install does not give. Not `resolve('oxlint/bin/oxlint')`
 * either — oxlint has an `exports` map that does not list that subpath, so
 * requesting it directly throws ERR_PACKAGE_PATH_NOT_EXPORTED. `./package.json`
 * is exported, and its `bin` says where the entry is.
 */
function resolveOxlint() {
  const require = createRequire(import.meta.url);
  const manifestPath = require.resolve('oxlint/package.json');
  const {bin} = JSON.parse(readFileSync(manifestPath, 'utf8'));
  const entry = typeof bin === 'string' ? bin : bin?.oxlint;
  if (typeof entry !== 'string' || entry.length === 0) {
    throw new Error(`no usable oxlint bin in ${manifestPath}`);
  }
  const packageDir = dirname(manifestPath);
  const resolved = resolve(packageDir, entry);
  if (!resolved.startsWith(packageDir)) {
    throw new Error(`oxlint bin points outside its package: ${entry}`);
  }
  return resolved;
}

const OXLINT = resolveOxlint();

const fixtureRoots = [];

afterEach(() => {
  // Each case writes a workspace to run the real linter against; without this
  // they accumulate in the temp directory for the life of the machine.
  for (const root of fixtureRoots.splice(0)) {
    rmSync(root, {recursive: true, force: true});
  }
});

/**
 * Lints `files` (path relative to the fixture root -> source) with only
 * no-select-star enabled. Returns the exit status and the files the rule
 * reported, as POSIX paths.
 */
function lintFixture(files) {
  const root = mkdtempSync(join(tmpdir(), 'zero-plugin-smoke-'));
  fixtureRoots.push(root);
  for (const [relativePath, source] of Object.entries(files)) {
    const filePath = join(root, relativePath);
    mkdirSync(dirname(filePath), {recursive: true});
    writeFileSync(filePath, source);
  }

  writeFileSync(
    join(root, '.oxlintrc.json'),
    JSON.stringify({
      plugins: ['eslint', 'typescript'],
      jsPlugins: [PLUGIN],
      categories: {
        correctness: 'off',
        suspicious: 'off',
        pedantic: 'off',
        style: 'off',
        restriction: 'off',
        nursery: 'off',
        perf: 'off',
      },
      rules: {
        'zero/no-select-star': [
          'error',
          {include: ['packages/zero-cache/src/']},
        ],
      },
    }),
  );

  const result = spawnSync(process.execPath, [OXLINT, '.', '--format=json'], {
    cwd: root,
    encoding: 'utf8',
  });
  let report;
  try {
    report = JSON.parse(result.stdout ?? '');
  } catch (cause) {
    throw new Error(
      `oxlint did not emit a JSON report (status ${result.status}): ` +
        `${result.stdout ?? ''}${result.stderr ?? ''}`,
      {cause},
    );
  }
  const reported = (report.diagnostics ?? [])
    .filter(diagnostic => diagnostic.code === 'zero(no-select-star)')
    .map(diagnostic => diagnostic.filename.replaceAll('\\', '/'));
  return {status: result.status, reported};
}

const selectStar = 'const sql = `SELECT * FROM issues`;\nexport default sql;\n';

test('no-select-star reports SELECT * inside the included paths, and only there', () => {
  const {status, reported} = lintFixture({
    'packages/zero-cache/src/query.ts': selectStar,
    'packages/other/src/query.ts': selectStar,
  });

  // The in-scope file is the canary for the out-of-scope one: its report proves
  // the plugin loaded and ran in this same run, so the other file's silence
  // comes from the include prefix and not from a plugin that never loaded. On
  // Windows the in-scope half is also the regression test: the prefix did not
  // match backslash paths, so the rule reported nothing anywhere.
  expect(reported).toEqual(['packages/zero-cache/src/query.ts']);
  expect(status).not.toBe(0);
});
