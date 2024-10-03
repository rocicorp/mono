import type {Schema} from 'zero-client';
import type {Database} from 'zqlite/src/db.js';

/**
 * Configuration for [[ZqlLiteZero]].
 */
export interface ZQLiteZeroOptions<S extends Schema> {
  schema: S;
  db: Database;
}
