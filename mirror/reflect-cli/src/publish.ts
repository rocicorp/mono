/* eslint-disable @typescript-eslint/naming-convention */
import {fetch} from 'undici';
import {readFileSync} from 'fs';
import type {
  CommonYargsArgv,
  StrictYargsOptionsToInterface,
} from './yarg-types.js';

export function publishOptions(yargs: CommonYargsArgv) {
  return yargs.option('name', {
    describe: 'Name of the worker',
    type: 'string',
    requiresArg: true,
  });
}

import FormData from 'form-data';

export async function publishHandler(
  yargs: StrictYargsOptionsToInterface<typeof publishOptions>,
) {
  const resolvedEntryPointPath = './example/customer.ts.example';
  const content = readFileSync(resolvedEntryPointPath);

  yargs.name = yargs.name || 'customer';

  const formData = new FormData();
  formData.append('bundle', content, {
    filename: 'customer.ts',
    contentType: 'text/plain',
  });

  await fetch(`http://127.0.0.1:5001/reflect-mirror-dev/us-central1/publish`, {
    method: 'POST',
    body: formData,
    headers: formData.getHeaders(),
  });
}
