import assert from 'node:assert';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {pkgUpSync} from 'pkg-up';

export function getVersion() {
  const require = createRequire(import.meta.url);
  const path = require.resolve('@rocicorp/reflect');
  const pkg = pkgUpSync({cwd: path});
  assert(pkg);
  return JSON.parse(readFileSync(pkg, 'utf-8')).version;
}
