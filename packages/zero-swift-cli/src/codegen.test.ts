import {mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {expect, test} from 'vitest';
import {cli, codegenFile, legacyArgv} from './cli.ts';
import type {Config} from './generate.ts';

const repo = fileURLToPath(new URL('../../../', import.meta.url));
const update = process.env['UPDATE_GENERATED'] === '1';

/**
 * The Swift tests compile and run these generated files, so they are checked
 * in; this keeps them current with the generator and the schemas.
 */
const fixtures: {name: string; dir?: string; config: Config}[] = [
  {
    name: 'zql test schema',
    config: {
      schema: 'packages/zql/src/query/test/test-schemas.ts',
      output: 'swift/Tests/ZeroTests/Generated/TestSchema.swift',
      schemaName: 'test',
    },
  },
  {
    name: 'zql test schema for the conformance tests',
    config: {
      schema: 'packages/zql/src/query/test/test-schemas.ts',
      output: 'swift/Tests/ConformanceTests/Generated/TestSchema.swift',
      schemaName: 'test',
      zql: 'testZQL',
      typePrefix: 'Test',
      access: 'internal',
    },
  },
  {
    name: 'zql test schema for the end-to-end tests',
    config: {
      schema: 'packages/zql/src/query/test/test-schemas.ts',
      output: 'swift/Tests/E2ETests/Generated/TestSchema.swift',
      schemaName: 'test',
      zql: 'testZQL',
      typePrefix: 'Test',
      access: 'internal',
    },
  },
  {
    name: 'zql test schema for the virtual list tests',
    config: {
      schema: 'packages/zql/src/query/test/test-schemas.ts',
      output: 'swift/Tests/ZeroVirtualTests/Generated/TestSchema.swift',
      schemaName: 'test',
      zql: 'testZQL',
      typePrefix: 'Test',
      access: 'internal',
    },
  },
  {
    name: 'zql test schema for the virtual list test app',
    config: {
      schema: 'packages/zql/src/query/test/test-schemas.ts',
      output: 'swift/TestHost/App/Generated/TestSchema.swift',
      schemaName: 'test',
      zql: 'testZQL',
      typePrefix: 'Test',
      access: 'internal',
    },
  },
  {
    name: 'codegen fixture schema',
    config: {
      schema: 'packages/zero-swift-cli/test/fixture-schema.ts',
      output: 'swift/Tests/ZeroTests/Generated/FixtureSchema.swift',
      schemaName: 'fixture',
      zql: 'fixtureZQL',
      typePrefix: 'Fx',
      tables: {project: {columns: {position: {type: 'int'}}}},
    },
  },
  {
    name: 'zbugs schema',
    dir: 'apps/zbugs-swift/',
    config: JSON.parse(
      readFileSync(repo + 'apps/zbugs-swift/zero-swift.json', 'utf8'),
    ) as Config,
  },
];

for (const {name, dir = '', config} of fixtures) {
  test(`generated Swift for the ${name} is current`, async () => {
    const swift = await codegenFile(config, repo + dir);
    const path = repo + dir + config.output;
    if (update) {
      writeFileSync(path, swift);
    }
    expect(swift).toBe(readFileSync(path, 'utf8'));
  });
}

test('a missing schema is a usage error, not a crash', async () => {
  let out = '';
  let code: number | undefined;
  // 0.1.0's `--schema <path>` still names the schema module.
  await cli.serve(
    legacyArgv(['codegen', '--schema', 'no-such-schema.ts', '--json']),
    {
      stdout: s => (out += s),
      exit: c => (code = c),
    },
  );
  expect(code).toBe(1);
  expect(JSON.parse(out)).toMatchObject({
    code: 'SCHEMA_NOT_FOUND',
    message: `Schema no-such-schema.ts doesn't exist. Pass --schema-file, or set "schema" in zero-swift.json.`,
  });
});

test('camelCase options from before 0.4.3 still work', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zero-swift-cli-'));
  const out = join(dir, 'Schema.swift');
  try {
    let code: number | undefined;
    await cli.serve(
      legacyArgv([
        'codegen',
        `--schemaFile=${fileURLToPath(new URL('../test/fixture-schema.ts', import.meta.url))}`,
        '--typePrefix',
        'Kx',
        '--schemaName=kx',
        '--out',
        out,
      ]),
      {stdout: () => {}, exit: c => (code = c)},
    );
    expect(code ?? 0).toBe(0);
    const swift = readFileSync(out, 'utf8');
    expect(swift).toContain('public struct KxProject');
    expect(swift).toContain('static let kx');
  } finally {
    rmSync(dir, {recursive: true, force: true});
  }
});
