import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {Database} from './db.ts';

/**
 * Configuration for [[ZqlLiteZero]].
 */
export interface ZQLiteZeroOptions<S extends Schema> {
  schema: S;
  db: Database;
}
