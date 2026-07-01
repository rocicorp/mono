// oxlint-disable no-console

import {execFileSync} from 'node:child_process';
import {mkdirSync} from 'node:fs';
import {mustEnv, writeGithubOutput} from './shared.ts';

const packDestination = mustEnv('PACK_DEST');
mkdirSync(packDestination, {recursive: true});

const output = execFileSync(
  'npm',
  ['pack', '--pack-destination', packDestination, '--json'],
  {encoding: 'utf8'},
);
const [{filename}] = JSON.parse(output) as [{filename?: unknown}];
if (typeof filename !== 'string' || filename.length === 0) {
  throw new Error(`Could not parse npm pack output: ${output}`);
}

writeGithubOutput({filename});
console.log(`Packed ${filename}`);
