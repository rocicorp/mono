import fs, {existsSync} from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {writePackageJson} from './app-config.js';
import {findReflectVersion} from './version.js';

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

export function scaffold(appName: string, dest: string) {
  const reflectVersion = findReflectVersion();
  copyTemplate('create', dest);
  writePackageJson({appName, reflectVersion}, dest, false);
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
