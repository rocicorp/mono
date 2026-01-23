import {expect, test} from 'vitest';
import {h64} from '../../../../../shared/src/hash.ts';
import * as v0 from './current.ts';
import {changeStreamMessageSchema} from './current/downstream.ts';
import {CHANGE_SOURCE_PATH} from './current/path.ts';
import {changeSourceUpstreamSchema} from './current/upstream.ts';

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
    CHANGE_SOURCE_PATH,
  };

  // Before making a breaking change to the protocol
  // (which may be indicated by a new hash),
  // copy the files in `current/` to the an appropriate
  // `v#/` directory and very that that hash did not change.
  // Then update the version number of the `CHANGE_SOURCE_PATH`
  // in current and export it appropriately as the new version
  // in `mod.ts`.
  t(current, '3uab2fcgb4oxj', '/changes/v0/stream');
  // During initial development, we use v0 as a non-stable
  // version (i.e. breaking change are allowed). Once the
  // protocol graduates to v1, versions must be stable.
  t(v0, '3uab2fcgb4oxj', '/changes/v0/stream');
});
