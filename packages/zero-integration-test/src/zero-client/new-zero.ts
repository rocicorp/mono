import {LogContext} from '@rocicorp/logger';
import {Zero} from '../../../zero-client/src/mod.js';
import {nanoid} from '../../../zero-client/src/util/nanoid.js';
import type {Schema} from '../../../zero-schema/src/mod.js';

export function newZero<S extends Schema>(_lc: LogContext, schema: S): Zero<S> {
  const z = new Zero({
    userID: 'user-' + nanoid(),
    schema,
  });
  return z;
}

export type Z = ReturnType<typeof newZero>;
