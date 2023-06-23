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
  for (const name of ['client', 'server', 'shared']) {
    for (const ext of ['js', 'd.ts', 'js.map']) {
      const src = basePath(
        '..',
        `reflect-${name}`,
        'out',
        `reflect-${name + '.' + ext}`,
      );
      const dst = basePath((name === 'shared' ? 'index' : name) + '.' + ext);
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

copyFiles();
