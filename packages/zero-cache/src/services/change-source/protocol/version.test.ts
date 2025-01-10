import {expect, test} from 'vitest';
import {h64} from '../../../../../shared/src/hash.js';
import {changeStreamMessageSchema} from './current/downstream.js';
import {CHANGE_SOURCE_PATH} from './current/path.js';
import {changeSourceUpstreamSchema} from './current/upstream.js';
import {v0} from './mod.js';

function t(
  module: {
    changeStreamMessageSchema: unknown;
    changeSourceUpstreamSchema: unknown;
    ['CHANGE_SOURCE_PATH']: string;
  },
  hash: string,
  path: string,
) {
  const h = h64(
    JSON.stringify(module.changeStreamMessageSchema) +
      JSON.stringify(module.changeSourceUpstreamSchema),
  ).toString(36);

  expect(h).toBe(hash);
  expect(module['CHANGE_SOURCE_PATH']).toBe(path);
}

test('protocol versions', () => {
  const current = {
    changeStreamMessageSchema,
    changeSourceUpstreamSchema,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    CHANGE_SOURCE_PATH,
  };

  // Before making a breaking change to the protocol
  // (which may be indicated by a new hash),
  // copy the files in `current/` to the an appropriate
  // `v#/` directory and very that that hash did not change.
  // Then update the version number of the `CHANGE_SOURCE_PATH`
  // in current and export it appropriately as the new version
  // in `mod.ts`.
  t(current, '1wkotqe19ed3k', '/changes/v0/stream');
  t(v0, '1wkotqe19ed3k', '/changes/v0/stream');
});
