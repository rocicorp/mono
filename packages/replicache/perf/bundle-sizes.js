/* eslint-env node, es2020 */

import commandLineArgs from 'command-line-args';
import commandLineUsage from 'command-line-usage';
import * as path from 'path';
import {promises as fs} from 'fs';

async function main() {
  const optionDefinitions = [
    {
      name: 'bundles',
      multiple: true,
      defaultValue: ['replicache.js', 'replicache.mjs'],
      description: 'bundle files whose sizes should be included in output',
    },
    {
      name: 'dir',
      defaultValue: './out',
      description: 'directory where bundles are located',
    },
    {
      name: 'help',
      alias: 'h',
      type: Boolean,
      description: 'Show this help message',
    },
  ];
  const options = commandLineArgs(optionDefinitions);
  if (options.help) {
    console.log(
      commandLineUsage([
        {content: 'Usage: module-sizes [options...]'},
        {optionList: optionDefinitions},
      ]),
    );
    process.exit();
  }

  const jsonEntries = [];
  for (const bundle of options.bundles) {
    const stats = await fs.stat(path.join(options.dir, bundle));
    jsonEntries.push({
      name: `Size of ${bundle}${
        bundle.endsWith('.br') ? ' (Brotli compressed)' : ''
      }`,
      unit: 'bytes',
      value: stats.size,
    });
  }

  process.stdout.write(JSON.stringify(jsonEntries, undefined, 2) + '\n');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
