import {LogContext} from '@rocicorp/logger';
import {test} from 'vitest';
import {type SchemaDefs, Zero} from 'zero-client/src/client/zero.js';
import {ZQLiteZero} from 'zqlite/src/zqlite-zero.js';

type CreateZeroFunction = <SD extends SchemaDefs>(
  lc: LogContext,
  z: SD,
) => Zero<SD> | ZQLiteZero<SD>;

export function runTests(_createZeroFunction: CreateZeroFunction) {
  test('no tests', () => {});
}
