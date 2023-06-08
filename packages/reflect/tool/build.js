// @ts-check

import * as fs from 'node:fs';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';

/** @param {string[]} parts */
function basePath(...parts) {
  return path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    '..',
    ...parts,
  );
}

function copyFiles() {
  for (const name of ['client', 'server']) {
    for (const ext of ['js', 'd.ts', 'js.map']) {
      const src = basePath(
        '..',
        `reflect-${name}`,
        'out',
        `reflect-${name + '.' + ext}`,
      );
      const dst = basePath(name + '.' + ext);
      if (!fs.existsSync(src)) {
        console.error(
          `File does not exist: ${src}. Make sure to build reflect-${name} first`,
        );
        process.exit(1);
      }

      fs.copyFileSync(src, dst);
    }
  }
}

function createVersionFiles() {
  const {version} = JSON.parse(
    fs.readFileSync(basePath('package.json'), 'utf8'),
  );

  fs.writeFileSync(
    basePath('index.js'),
    `/**@type{string}*/export const version=${JSON.stringify(version)}\n`,
  );

  fs.writeFileSync(
    basePath('index.d.ts'),
    'declare const version: string;\nexport{version};\n',
  );
}

createVersionFiles();
copyFiles();
