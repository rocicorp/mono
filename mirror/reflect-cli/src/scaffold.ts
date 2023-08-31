import fs, {existsSync} from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {readFile} from 'node:fs/promises';
import {pkgUp} from 'pkg-up';
import {assert, assertObject, assertString} from 'shared/src/asserts.js';

const templateDir = path.resolve(
  fileURLToPath(import.meta.url),
  '../..',
  `template`,
);

const templateBinDir = path.resolve(
  fileURLToPath(import.meta.url),
  '../..',
  'bin',
  `template`,
);

const TEMPLATED_FILES = [
  'package.json',
  '.env',
  'reflect.config.json',
] as const;

export async function scaffold(name: string, dest: string): Promise<void> {
  const reflectVersion = await findReflectVersion();
  const sourceDir = existsSync(templateDir) ? templateDir : templateBinDir;

  copyDir(sourceDir, dest);
  writeTemplatedFilePlaceholders(dest, {
    ['<APP-NAME>']: name,
    ['<REFLECT-VERSION>']: reflectVersion,
  });
}

export function writeTemplatedFilePlaceholders(
  dest: string,
  placeholders: Record<string, string>,
) {
  TEMPLATED_FILES.forEach(file => {
    editFile(path.resolve(dest, file), content => {
      for (const [key, value] of Object.entries(placeholders)) {
        content = content.replaceAll(key, value);
      }
      return content;
    });
  });
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

function editFile(file: string, callback: (content: string) => string) {
  const content = fs.readFileSync(file, 'utf-8');
  fs.writeFileSync(file, callback(content), 'utf-8');
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
