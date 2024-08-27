import type {SchemaDefs} from 'zero-client/src/client/zero.js';
import type {Database} from 'better-sqlite3';

/**
 * Configuration for [[ZqlLiteZero]].
 */
export interface ZQLiteZeroOptions<SD extends SchemaDefs> {
  schemas?: SD | undefined;
  db: Database;
}
