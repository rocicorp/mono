import {ZqlLiteZero} from 'zqlite/src/zqlite-zero.js';
import {createTest as countIntegrationTest} from './count-integration.js';
import {createTest as distinctIntegrationTest} from './distinct-integration.js';
import {QueryDefs, Zero} from 'zero-client/src/client/zero.js';
import {QueryParseDefs} from 'zero-client/src/client/options.js';

type CreateZeroFunction = <QD extends QueryDefs>(
  z: QueryParseDefs<QD>,
) => Zero<QD> | ZqlLiteZero<QD>;

export function runTests(createZeroFunction: CreateZeroFunction) {
  countIntegrationTest(createZeroFunction);
  distinctIntegrationTest(createZeroFunction);
}
