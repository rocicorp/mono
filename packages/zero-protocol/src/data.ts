import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';

export const valueSchema = v.union(jsonSchema, v.undefined());
export const rowSchema = v.readonlyRecord(valueSchema);
