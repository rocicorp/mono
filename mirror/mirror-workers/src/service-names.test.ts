import {test} from '@jest/globals';
import {opendir} from 'node:fs/promises';
import {resolve} from 'path';
import {workerNameSchema} from './service-names.js';

test('worker names correspond to directories', async () => {
  for (const workerName of workerNameSchema.options.map(
    t => (t as unknown as {value: string}).value,
  )) {
    await opendir(resolve('./src/', workerName));
  }
});
