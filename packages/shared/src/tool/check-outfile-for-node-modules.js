// @ts-check

import * as fs from 'fs/promises';

/**
 * Checks if the file contains bundle node_modules modules.
 * @param {string} outfile Path to the file
 */
export async function checkOutfileForNodeModules(outfile) {
  const file = await fs.readFile(outfile, 'utf-8');
  for (const line of file.split('\n')) {
    if (line.includes('node_modules/jest/bin/jest.js')) {
      continue;
    }
    if (line.includes('node_modules')) {
      throw new Error('node_modules found in outfile:\n  ' + line);
    }
  }
}
