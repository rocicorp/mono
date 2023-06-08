import {readFileSync} from 'fs';

import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function publishOptions(yargs: CommonYargsArgv) {
  return yargs.option('name', {
    describe: 'Name of the worker',
    type: 'string',
    requiresArg: true,
  });
}

export async function publishHandler(
  yargs: YargvToInterface<ReturnType<typeof publishOptions>>,
) {
  const resolvedEntryPointPath = './example/customer.ts.example';
  const content = readFileSync(resolvedEntryPointPath, 'utf-8');

  yargs.name = yargs.name || 'customer';

  const formData = new FormData();
  formData.append('bundle', content, 'customer.ts');

  await fetch(
    `http://127.0.0.1:5001/reflect-mirror-staging/us-central1/publish`,
    {
      method: 'POST',
      body: formData,
    },
  );
}
