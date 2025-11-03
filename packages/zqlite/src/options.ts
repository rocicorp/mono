import type {Schema} from '../../zero-types/src/schema.ts';
import type {Database} from './db.ts';

/**
 * Configuration for [[ZqlLiteZero]].
 */
export interface ZQLiteZeroOptions<S extends Schema> {
  schema: S;
  db: Database;
}
