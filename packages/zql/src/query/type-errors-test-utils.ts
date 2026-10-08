import {spawnSync} from 'node:child_process';
import {mkdtempSync, realpathSync, rmSync, writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import {dirname, join, relative} from 'node:path';
import {fileURLToPath} from 'node:url';

// A new error starts a line; its continuation lines are indented.
const errorStart = /\n(?=\S)/;

/**
 * Compiles a module with tsc and returns the errors reported in it, each with
 * its continuation lines.
 *
 * `@ts-expect-error` only proves that *an* error fires. Use this when the test
 * is about what the error says.
 *
 * @param lines Builds the module's source. `from` turns a path relative to
 * this directory into an import specifier for it.
 */
export function typeErrors(
  lines: (from: (path: string) => string) => readonly string[],
): string[] {
  // Real path, so a relative import from it resolves where `relative` says
  // even when the temp dir sits behind a symlink, as it does on macOS.
  const dir = realpathSync(mkdtempSync(join(tmpdir(), 'type-errors-')));
  try {
    // Relative, as `rewriteRelativeImportExtensions` rejects an absolute
    // `.ts` import.
    const from = (path: string) =>
      JSON.stringify(
        relative(dir, fileURLToPath(new URL(path, import.meta.url))),
      );
    writeFileSync(join(dir, 'misuse.ts'), lines(from).join('\n'));
    writeFileSync(
      join(dir, 'tsconfig.json'),
      JSON.stringify({
        extends: fileURLToPath(new URL('../../tsconfig.json', import.meta.url)),
        compilerOptions: {types: [], incremental: false},
        include: [],
        files: ['misuse.ts'],
      }),
    );
    const tsc = join(
      dirname(
        createRequire(import.meta.url).resolve('typescript/package.json'),
      ),
      'bin/tsc',
    );
    const {stdout} = spawnSync(process.execPath, [tsc, '--pretty', 'false'], {
      cwd: dir,
      encoding: 'utf8',
    });
    return stdout
      .split(errorStart)
      .filter(error => error.startsWith('misuse.ts'));
  } finally {
    rmSync(dir, {recursive: true, force: true});
  }
}
