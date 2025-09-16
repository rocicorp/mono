/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
  t(current, 'hfkz08pmaavz', '/changes/v0/stream');
  // During initial development, we use v0 as a non-stable
  // version (i.e. breaking change are allowed). Once the
  // protocol graduates to v1, versions must be stable.
  t(v0, 'hfkz08pmaavz', '/changes/v0/stream');
});
