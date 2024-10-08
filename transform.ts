import {glob} from 'glob';
import {existsSync, statSync} from 'node:fs';
import {readFile, writeFile} from 'node:fs/promises';
import {dirname, relative} from 'node:path';
import process, {argv} from 'node:process';

if (argv.length !== 3) {
  console.log('Usage: transform.js <pattern>');
  process.exit(1);
}

const packagePattern = argv[2];

const files = await glob(packagePattern, {
  nodir: true,
  ignore: ['**/node_modules/**', '**/dist/**', '**/out/**'],
});

if (files.length === 0) {
  console.log('Pattern does not match any files');
  process.exit(1);
}

async function transformFile(path: string) {
  // path starts with packages/replicache/src/
  let s: string;
  // vitest creates directories that has the name xxx.test.ts for its __screenshots__ directory
  // Make sure we have a file and not a directory
  if (!isFile(path)) {
    console.log(`Skipping non file path: ${path}`);
    return;
  }
  try {
    s = await readFile(path, 'utf8');
  } catch (e) {
    console.error('Error reading file', path, e);
    throw e;
  }
  const re =
    /(from|import)\s+'((?:btree|datadog|replicache|shared|zero-cache|zero-client|zero-protocol|zero-react|zero|zql|zqlite)\/src\/.+)'/g;
  const absDir = new URL(dirname(path), import.meta.url).href;
  const s2 = s.replace(re, (_, fromOrImport, m) => {
    const sharedURL = new URL(`./packages/${m}`, import.meta.url).href;
    let rel = relative(absDir, sharedURL);
    if (!rel.startsWith('./') && !rel.startsWith('../')) {
      rel = './' + rel;
    }
    assertFileExists(rel, absDir + '/', path);
    console.log(`Changed from ${m} to ${rel}`);
    return `${fromOrImport} '${rel}'`;
  });
  if (s !== s2) {
    console.log('Writing', path);
    await writeFile(path, s2);
    console.log();
  }
}

for (const file of files.slice(0, 60)) {
  await transformFile(file);
}

function assertFileExists(relPath: string, base: string, importer: string) {
  if (
    existsSync(new URL(replaceExtension(relPath), base)) ||
    existsSync(new URL(relPath, base))
  ) {
    return;
  }
  throw new Error(
    `File does not exist: ${relPath}. Referenced from: ${importer}`,
  );
}

function replaceExtension(relPath: string): string {
  return relPath.replace(/\.js(x?)/, '.ts$1');
}

function isFile(path: string): boolean {
  const stat = statSync(path, {throwIfNoEntry: false});
  if (!stat) {
    return false;
  }
  return stat.isFile();
}
