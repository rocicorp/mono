import type {SchemaDefs} from 'zero-client/src/client/zero.js';
import type {Database} from 'better-sqlite3';

/**
 * Configuration for [[ZqlLiteZero]].
 */
export interface ZqlLiteZeroOptions<QD extends SchemaDefs> {
  schemas?: QD | undefined;
  db: Database;
}
