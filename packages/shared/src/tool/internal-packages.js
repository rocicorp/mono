// @ts-check

/* eslint-env node, es2022 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';

/**
 * Map from name to packages/name or mirror/name
 * @type {Map<string, string>}
 */
export const internalPackagesMap = new Map();

const monoRootPath = fileURLToPath(new URL('../../../../', import.meta.url));

for (const p of ['packages', 'mirror']) {
  for (const f of fs.readdirSync(path.join(monoRootPath, p))) {
    const stat = fs.statSync(path.join(monoRootPath, p, f));
    if (stat.isDirectory()) {
      // Also ensure that there is a package.json in that directory
      const packageJSONPath = path.join(monoRootPath, p, f, 'package.json');

      if (fs.existsSync(packageJSONPath)) {
        internalPackagesMap.set(f, `${p}/${f}`);
      }
    }
  }
}

export const internalPackages = [...internalPackagesMap.keys()];

/**
 * @param {string} name
 */
export function isInternalPackage(name) {
  return internalPackagesMap.has(name);
}
