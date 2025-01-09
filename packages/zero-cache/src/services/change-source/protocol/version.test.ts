import {expect, test} from 'vitest';
import {h64} from '../../../../../shared/src/hash.js';
import {changeSourceDownstreamSchema} from './current/downstream.js';
import {CHANGE_SOURCE_PATH} from './current/path.js';
import {changeSourceUpstreamSchema} from './current/upstream.js';
import {v0} from './mod.js';

test('protocol version', () => {
  const hash = h64(
    JSON.stringify(changeSourceDownstreamSchema) +
      JSON.stringify(changeSourceUpstreamSchema),
  ).toString(36);

  // If this test fails because the change-source/protocol has changed such that
  // old code will not understand the new schema, make a copy of the previous
  // current/ into a v#/ archive that is exported in mod.ts appropriately.
  // Then bump the version of the `CHANGE_SOURCE_PATH` and reference current/*
  // as the new version in mod.ts.
  expect(hash).toBe('1wkotqe19ed3k');
  expect(CHANGE_SOURCE_PATH).toBe('/changes/v0/stream');
});

test('paths', () => {
  expect(v0.CHANGE_SOURCE_PATH).toBe('/changes/v0/stream');
});
