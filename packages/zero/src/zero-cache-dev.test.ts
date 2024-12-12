import {expect, test} from 'vitest';
import {
  buildSchemaOptions,
  ZERO_BUILD_SCHEMA_ENV_VAR_PREFIX,
} from '../../zero-schema/src/build-schema-options.js';
import {
  ZERO_ENV_VAR_PREFIX,
  zeroOptions,
} from '../../zero-cache/src/config/zero-config.js';
import type {Group} from '../../shared/src/options.js';
test('zeroOptions and buildSchemaOptions are compatible', () => {
  expect(ZERO_BUILD_SCHEMA_ENV_VAR_PREFIX).toEqual(ZERO_ENV_VAR_PREFIX);

  // buildSchemaOptions has a single Group called schema
  expect(Object.keys(buildSchemaOptions)).toEqual(['schema']);
  expect(buildSchemaOptions.schema).toBeInstanceOf(Object);
  buildSchemaOptions.schema satisfies Group;

  // zeroOptions also has a schema group
  expect(zeroOptions.schema).toBeInstanceOf(Object);
  zeroOptions.schema satisfies Group;

  // buildSchemaOptions.schema's keys have no overlap
  // with zeroOptions.schema's keys
  for (const key of Object.keys(buildSchemaOptions.schema)) {
    expect(Object.hasOwn(zeroOptions.schema, key), key).false;
  }
});
