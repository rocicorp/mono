import * as v from '../../../shared/src/valita.js';
import type {PrimaryKeyValueRecord} from '../../../zero-protocol/src/primary-key.js';
import {
  primaryKeyValueSchema,
  type PrimaryKey,
  type PrimaryKeyValue,
} from '../../../zero-protocol/src/primary-key.js';
import type {Value} from '../../../zql/src/zql/ivm/data.js';

export function makeIDFromPrimaryKey(
  primaryKey: PrimaryKey,
  value: Record<string, Value>,
): PrimaryKeyValueRecord {
  const id: Record<string, PrimaryKeyValue> = {};
  for (const key of primaryKey) {
    id[key] = v.parse(value[key], primaryKeyValueSchema);
  }
  return id;
}
