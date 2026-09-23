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
 * Smoke tests for the rules as the linter runs them, not for their predicates.
 *
 * The unit tests next door cover the predicates directly. They cannot catch the
 * failure that actually happened: no-select-star's include prefixes never
 * matched, so the rule quietly did not run and the only symptom was an
 * "unused disable directive" elsewhere. A rule that cannot report is
 * indistinguishable from a clean codebase, so each rule is exercised end to end
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

/**
 * Rules from THIS plugin that always report, with a file that violates each.
 *
 * A built-in rule would prove only that oxlint ran. oxlint's plugin loader
 * returns a failure rather than throwing, so the day it warns-and-continues
 * instead of aborting, a built-in canary would still fire while none of the
 * plugin's rules existed — and every "stays quiet" assertion below would pass
 * over a plugin that never loaded.
 *
 * Two, so the canary is never the rule under test: a rule cannot vouch for its
 * own registration while also being what the case is measuring.
 */
const CANARIES = [
  {
    rule: 'zero/no-select-star',
    file: join('packages', 'zero-cache', 'src', '__canary_select.ts'),
    source: 'export const q = `SELECT * FROM t`;\n',
    options: {include: ['packages/zero-cache/src/']},
  },
  {
    rule: 'zero/no-bare-shim-spawn',
    file: '__canary_spawn.ts',
    source:
      "import {spawnSync} from 'node:child_process';\n" +
      "export const r = spawnSync('npm', ['--version']);\n",
  },
];

/** The canary for a case, which is any of them that is not under test. */
function canaryFor(ruleName) {
  const canary = CANARIES.find(candidate => candidate.rule !== ruleName);
  if (canary === undefined) {
    throw new Error(`no canary available while testing ${ruleName}`);
  }
  return canary;
}

/** oxlint's `--format=json` report, as the codes and severities it reported. */
function parseReport(stdout, context) {
  let report;
  try {
    report = JSON.parse(stdout);
  } catch (cause) {
    throw new Error(`oxlint did not emit a JSON report: ${context}`, {cause});
  }
  const diagnostics = report.diagnostics ?? [];
  return {
    // `zero(no-select-star)` — the plugin name oxlint reports a rule under.
    codes: diagnostics.map(diagnostic => diagnostic.code),
    errors: diagnostics
      .filter(diagnostic => diagnostic.severity === 'error')
      .map(diagnostic => diagnostic.code),
    filesLinted: report.number_of_files ?? 0,
  };
}

/** The code oxlint reports a rule under, from the name a config enables it by. */
function codeOf(ruleName) {
  const [plugin, rule] = ruleName.split('/');
  return rule === undefined ? `eslint(${plugin})` : `${plugin}(${rule})`;
}

const fixtureRoots = [];

afterEach(() => {
  // Each case writes a workspace to run the real linter against; without this
  // they accumulate in the temp directory for the life of the machine.
  for (const root of fixtureRoots.splice(0)) {
    rmSync(root, {recursive: true, force: true});
  }
});

/** Writes a fixture file plus a config enabling only this plugin's rules. */
function lintFixture(relativeFilePath, source, ruleName, ruleOptions) {
  const root = mkdtempSync(join(tmpdir(), 'zero-plugin-smoke-'));
  fixtureRoots.push(root);
  const filePath = join(root, relativeFilePath);
  mkdirSync(dirname(filePath), {recursive: true});
  writeFileSync(filePath, source);

  // At WARNING level, so it never moves the exit code the assertions read. Its
  // diagnostic is the proof that this plugin's rules are registered and firing.
  const canary = canaryFor(ruleName);
  // Nested, because a rule scoped by an `include` prefix only fires on a path
  // inside it — so the canary has to sit where the rule actually looks.
  const canaryPath = join(root, canary.file);
  mkdirSync(dirname(canaryPath), {recursive: true});
  writeFileSync(canaryPath, canary.source);

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
        // The canary carries its own options: a rule scoped by `include` reports
        // nothing when enabled bare, and a silent canary is indistinguishable
        // from a plugin that never loaded — which is the one thing this harness
        // exists to tell apart.
        [canary.rule]:
          canary.options === undefined ? 'warn' : ['warn', canary.options],
        [ruleName]:
          ruleOptions === undefined ? 'error' : ['error', ruleOptions],
      },
    }),
  );

  const result = spawnSync(process.execPath, [OXLINT, '.', '--format=json'], {
    cwd: root,
    encoding: 'utf8',
  });
  const report = parseReport(
    result.stdout ?? '',
    `status ${result.status}: ${result.stderr ?? ''}`,
  );

  // A harness that cannot lint would satisfy every "does not report" assertion
  // below — a plugin that fails to load, a rejected config, a wrong oxlint path.
  // So each run proves the PLUGIN linted the fixture before its own assertion is
  // allowed to mean anything. This is the same failure the rule under test had
  // (silence is indistinguishable from clean), reintroduced one level up.
  if (!report.codes.includes(codeOf(canary.rule))) {
    throw new Error(
      `${canary.rule} did not report, so the plugin's rules are not running ` +
        `(status ${result.status}, ${report.filesLinted} file(s) linted): ` +
        `${result.stdout ?? ''}${result.stderr ?? ''}`,
    );
  }
  return {report, status: result.status};
}

/** Asserts the rule fired on a fixture that violates it. */
function expectReported(result, ruleName) {
  // The reported code, not a substring of oxlint's prose: the config it echoes on
  // a rejection contains the rule names, so a grep passes on a run that linted
  // nothing.
  expect(result.report.errors).toContain(codeOf(`zero/${ruleName}`));
  expect(result.status).not.toBe(0);
}

/** Asserts the rule stayed silent on a fixture that does not violate it. */
function expectSilent(result, ruleName) {
  expect(result.report.codes).not.toContain(codeOf(`zero/${ruleName}`));
  // The canary is a warning, so a clean fixture leaves the exit code at 0. That
  // is asserted rather than assumed: a change in how oxlint treats warnings would
  // otherwise turn every quiet case red and point at the plugin.
  expect(result.status).toBe(0);
}

test('no-select-star reports SELECT * inside an included path', () => {
  const result = lintFixture(
    join('packages', 'zero-cache', 'src', 'query.ts'),
    'const sql = `SELECT * FROM issues`;\nexport default sql;\n',
    'zero/no-select-star',
    {include: ['packages/zero-cache/src/']},
  );

  // The regression this guards: the include prefix stopped matching, the rule
  // silently did not run, and the lint reported a clean file.
  expectReported(result, 'no-select-star');
});

test('no-select-star stays quiet outside the included paths', () => {
  const result = lintFixture(
    join('packages', 'other', 'src', 'query.ts'),
    'const sql = `SELECT * FROM issues`;\nexport default sql;\n',
    'zero/no-select-star',
    {include: ['packages/zero-cache/src/']},
  );

  expectSilent(result, 'no-select-star');
});

test('no-bare-shim-spawn reports spawning a shim command by bare name', () => {
  const result = lintFixture(
    join('packages', 'thing', 'src', 'a.ts'),
    "import {spawn} from 'node:child_process';\nspawn('tsc', ['-p', 'tsconfig.json'], {stdio: 'inherit'});\n",
    'zero/no-bare-shim-spawn',
  );

  expectReported(result, 'no-bare-shim-spawn');
});

test('no-bare-shim-spawn leaves a real executable alone', () => {
  const result = lintFixture(
    join('packages', 'thing', 'src', 'a.ts'),
    "import {spawn} from 'node:child_process';\nspawn('git', ['--version'], {stdio: 'inherit'});\n",
    'zero/no-bare-shim-spawn',
  );

  expectSilent(result, 'no-bare-shim-spawn');
});
