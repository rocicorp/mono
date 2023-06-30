// @ts-check

// This script checks that the generated d.ts files do not contain any
// initializers in ambient contexts.
//
// TS1039: Initializers are not allowed in ambient contexts.

import {readFileSync} from 'fs';
import * as path from 'path';
import {fileURLToPath} from 'url';

const dtsFilename = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  '..',
  'out',
  'reflect-client.d.ts',
);

const content = readFileSync(dtsFilename, 'utf8');

// This test is not robust but it catches the one which was reported.
if (content.includes('new Lock()')) {
  console.error(`Found initializer in ambient context ${dtsFilename}`);
  process.exit(1);
}
