import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import color from 'picocolors';
import fs, {existsSync} from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import validateProjectName from 'validate-npm-package-name';

function isValidPackageName(projectName: string): string | void {
  const nameValidation = validateProjectName(projectName);
  if (!nameValidation.validForNewPackages) {
    return [
      ...(nameValidation.errors || []),
      ...(nameValidation.warnings || []),
    ].join('\n');
  }
}

export function scaffoldOptions(yargs: CommonYargsArgv) {
  return yargs.option('name', {
    describe: 'Name of the app',
    type: 'string',
    demandOption: true,
  });
}

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

type ScaffoldHandlerArgs = YargvToInterface<ReturnType<typeof scaffoldOptions>>;

export function scaffoldHandler(yargs: ScaffoldHandlerArgs) {
  const {name} = yargs;
  const invalidPackageNameReason = isValidPackageName(name);
  if (invalidPackageNameReason) {
    console.log(
      color.red(
        `Invalid project name: ${color.bgWhite(
          name,
        )} - (${invalidPackageNameReason})`,
      ),
    );
    process.exit(1);
  }
  console.log(color.green(`Creating folder: ${color.bgWhite(name)}`));

  const sourceDir = existsSync(templateDir) ? templateDir : templateBinDir;

  copyDir(sourceDir, name);

  updateProjectName(name);
  updateEnvFile(name, `wss://${name}.reflect-server.net`);
  console.log(color.green('Finished initializing your reflect project 🎉'));
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

function updateProjectName(targetDir: string) {
  const packageJsonPath = path.resolve(targetDir, 'package.json');
  editFile(packageJsonPath, content =>
    content.replace(
      /"name": "reflect-template-example"/,
      `"name": "${targetDir}"`,
    ),
  );
}

export function updateEnvFile(targetDir: string, workerUrl: string) {
  const envExamplePath = path.resolve(targetDir, '.env.example');
  const envFinal = path.resolve(targetDir, '.env');
  fs.copyFileSync(envExamplePath, envFinal);
  editFile(envFinal, content =>
    content.replace('VITE_WORKER_URL=', `VITE_WORKER_URL=${workerUrl}`),
  );
}
