import {LogContext} from '@rocicorp/logger';
import {test} from 'vitest';
import {Zero} from '../../zero-client/src/client/zero.js';
import {ZQLiteZero} from '../../zqlite/src/zqlite-zero.js';
import type {Schema} from '../../zero-schema/src/mod.js';

type CreateZeroFunction = <S extends Schema>(
  lc: LogContext,
  z: S,
) => Zero<S> | ZQLiteZero<S>;

export function runTests(_createZeroFunction: CreateZeroFunction) {
  test('no tests', () => {});
}
