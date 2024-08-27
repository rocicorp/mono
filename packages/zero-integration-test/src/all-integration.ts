import {ZqlLiteZero} from 'zqlite/src/zqlite-zero.js';
import {QueryDefs, Zero} from 'zero-client/src/client/zero.js';
import {test} from 'vitest';

type CreateZeroFunction = <QD extends QueryDefs>(
  z: QD,
) => Zero<QD> | ZqlLiteZero<QD>;

export function runTests(_createZeroFunction: CreateZeroFunction) {
  test('no tests', () => {});
}
