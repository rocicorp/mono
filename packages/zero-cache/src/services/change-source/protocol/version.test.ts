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
  t(current, 'r7gl2fs37kac', '/changes/v0/stream');
  // During initial development, we use v0 as a non-stable
  // version (i.e. breaking change are allowed). Once the
  // protocol graduates to v1, versions must be stable.
  t(v0, 'r7gl2fs37kac', '/changes/v0/stream');
});
