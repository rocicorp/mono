import fs, {existsSync} from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {readFile} from 'node:fs/promises';
import {pkgUp} from 'pkg-up';
import {assert, assertObject, assertString} from 'shared/src/asserts.js';
import {writeTemplatedFilePlaceholders} from './app-config.js';

const templateDir = (templateName: string) =>
  path.resolve(
    fileURLToPath(import.meta.url),
    '../..',
    `templates`,
    templateName,
  );

const templateBinDir = (templateName: string) =>
  path.resolve(
    fileURLToPath(import.meta.url),
    '../..',
    'bin',
    `templates`,
    templateName,
  );

export function copyTemplate(name: string, dest: string) {
  const sourceDir = existsSync(templateDir(name))
    ? templateDir(name)
    : templateBinDir(name);
  copyDir(sourceDir, dest);
}

export async function scaffold(appName: string, dest: string) {
  const reflectVersion = await findReflectVersion();
  copyTemplate('create', dest);
  writeTemplatedFilePlaceholders({appName, reflectVersion}, dest, false);
}

function copy(src: string, dest: string) {
  const stat = fs.statSync(src);
  if (stat.isDirectory()) {
    copyDir(src, dest);
  } else {
    fs.copyFileSync(src, dest);
  }
}

function copyDir(srcDir: string, destDir: string) {
  fs.mkdirSync(destDir, {recursive: true});
  for (const file of fs.readdirSync(srcDir)) {
    const srcFile = path.resolve(srcDir, file);
    const destFile = path.resolve(destDir, file);
    copy(srcFile, destFile);
  }
}

async function findReflectVersion(): Promise<string> {
  const pkg = await pkgUp({cwd: fileURLToPath(import.meta.url)});
  assert(pkg);
  const s = await readFile(pkg, 'utf-8');
  const v = JSON.parse(s);
  assertObject(v);
  assertString(v.version);
  return v.version;
}
