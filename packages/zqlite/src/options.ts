import type {Schema} from '../../zero-client/src/mod.js';
import type {Database} from './db.js';

/**
 * Configuration for [[ZqlLiteZero]].
 */
export interface ZQLiteZeroOptions<S extends Schema> {
  schema: S;
  db: Database;
}
