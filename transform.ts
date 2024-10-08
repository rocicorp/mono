import {glob} from 'glob';
import {existsSync} from 'node:fs';
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
  process.stdout.write(`Transforming ${path}... `);
  // path starts with packages/replicache/src/
  let s: string;

  try {
    s = await readFile(path, 'utf8');
  } catch (e) {
    console.error('Error reading file', path, e);
    throw e;
  }
  const re =
    /(from|import)\s+'((?:btree|datadog|replicache|shared|zero-cache|zero-client|zero-protocol|zero-react|zero|zql|zqlite)\/.+)'/g;
  const absDir = new URL(dirname(path), import.meta.url).href;
  const s2 = s.replace(re, (_, fromOrImport, m) => {
    const sharedURL = new URL(`./packages/${m}`, import.meta.url).href;
    let rel = relative(absDir, sharedURL);
    if (!rel.startsWith('./') && !rel.startsWith('../')) {
      rel = './' + rel;
    }
    assertFileExists(rel, absDir + '/', path);
    process.stdout.write(`\n  Changed from ${m} to ${rel}`);
    return `${fromOrImport} '${rel}'`;
  });
  if (s !== s2) {
    process.stdout.write(`\n  Wrote\n\n`);
    await writeFile(path, s2);
  } else {
    process.stdout.write(`No change\n`);
  }
}

for (const file of files) {
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
