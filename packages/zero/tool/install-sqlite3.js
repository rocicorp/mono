// @ts-check

import {execSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {cwd, stdout} from 'node:process';

if (isPartOfMonorepo()) {
  // Running in monorepo. Not doing npm install.
  stdout.write(
    'running @rocicorp/zero preinstall in monorepo. Not installing sqlite3 from source\n',
  );
} else {
  // Now run the npm install command
  execSync(
    `npm install better-sqlite3@11.1.2 --no-save --build-from-source --sqlite3="${cwd()}/deps/sqlite3"`,
    {
      stdio: 'inherit',
      cwd: cwd(),
    },
  );
}

function isPartOfMonorepo() {
  const monorepoPackageJSONPath = new URL(
    '../../../package.json',
    import.meta.url,
  ).pathname;
  try {
    const monorepoPackageJSON = JSON.parse(
      readFileSync(monorepoPackageJSONPath, 'utf8'),
    );
    if (monorepoPackageJSON.name === '@rocicorp/mono') {
      return true;
    }
  } catch {}
  return false;
}
