import type {QueryDefs} from 'zero-client/src/client/zero.js';
import type {Database} from 'better-sqlite3';

/**
 * Configuration for [[ZqlLiteZero]].
 */
export interface ZqlLiteZeroOptions<QD extends QueryDefs> {
  schemas?: QD | undefined;
  db: Database;
}
