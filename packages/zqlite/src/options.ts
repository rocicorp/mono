import type {QueryDefs} from 'zero-client/src/client/zero.js';
import type {Database} from 'better-sqlite3';
import {QueryParseDefs} from 'zero-client/src/client/options.js';

/**
 * Configuration for [[ZqlLiteZero]].
 */
export interface ZqlLiteZeroOptions<QD extends QueryDefs> {
  queries?: QueryParseDefs<QD> | undefined;
  db: Database;
}
