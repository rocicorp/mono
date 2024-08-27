import {ZQLiteZero} from 'zqlite/src/zqlite-zero.js';
import {SchemaDefs, Zero} from 'zero-client/src/client/zero.js';
import {test} from 'vitest';

type CreateZeroFunction = <SD extends SchemaDefs>(
  z: SD,
) => Zero<SD> | ZQLiteZero<SD>;

export function runTests(_createZeroFunction: CreateZeroFunction) {
  test('no tests', () => {});
}
