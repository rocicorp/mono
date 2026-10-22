/**
 * Compiles the Swift `zero-swift codegen` generates for each schema of
 * `test/sweep-schema.ts` against the local Swift package. Skipped where
 * `swift` isn't installed. The first run builds the Zero package, so it is
 * slow; later runs reuse the scratch directory.
 */
import {spawnSync} from 'node:child_process';
import {mkdirSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {beforeAll, expect, test} from 'vitest';
import * as schemas from '../test/sweep-schema.ts';
import {codegenFile} from './cli.ts';

const repo = fileURLToPath(new URL('../../../', import.meta.url));
const hasSwift = spawnSync('swift', ['--version']).status === 0;
const dir = join(tmpdir(), 'zero-swift-sweep-codegen');
const scratch = join(dir, '.build');
const names = Object.keys(schemas);

beforeAll(() => {
  if (!hasSwift) return;
  mkdirSync(dir, {recursive: true});
  const targets = names
    .map(
      n =>
        `    .target(name: "${n}", dependencies: [.product(name: "Zero", package: "swift")]),`,
    )
    .join('\n');
  writeFileSync(
    join(dir, 'Package.swift'),
    `// swift-tools-version: 6.2
import PackageDescription
let package = Package(
  name: "SweepCodegen", platforms: [.iOS(.v26), .macOS(.v26)],
  dependencies: [.package(path: ${JSON.stringify(repo + 'swift')})],
  targets: [
${targets}
  ])
`,
  );
  for (const n of names) {
    mkdirSync(join(dir, 'Sources', n), {recursive: true});
    writeFileSync(join(dir, 'Sources', n, 'Schema.swift'), '');
  }
  spawnSync(
    'swift',
    ['build', '--scratch-path', scratch, '--product', 'Zero'],
    {
      cwd: dir,
    },
  );
}, 600_000);

for (const name of names) {
  test.skipIf(!hasSwift)(
    `the generated Swift for ${name} compiles`,
    async () => {
      const swift = await codegenFile(
        {schema: 'packages/zero-swift-cli/test/sweep-schema.ts', export: name},
        repo,
      );
      writeFileSync(join(dir, 'Sources', name, 'Schema.swift'), swift);
      const r = spawnSync(
        'swift',
        ['build', '--scratch-path', scratch, '--target', name],
        {cwd: dir, encoding: 'utf8'},
      );
      const errors = (r.stdout + r.stderr)
        .split('\n')
        .filter(l => l.includes('error:') && l.startsWith('/'));
      expect([...new Set(errors)].join('\n')).toBe('');
      expect(r.status).toBe(0);
    },
    600_000,
  );
}
