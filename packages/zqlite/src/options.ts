import type {Schema} from '../../zero-schema/src/schema.js';
import type {Database} from './db.js';

/**
 * Configuration for [[ZqlLiteZero]].
 */
export interface ZQLiteZeroOptions<S extends Schema> {
  schema: S;
  db: Database;
}
