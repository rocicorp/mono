import * as valita from '@badrap/valita';
import {skipAssertJSONValue} from './config.ts';
import type {ReadonlyJSONObject, ReadonlyJSONValue} from './json.ts';
import {isJSONObject, isJSONValue} from './json.ts';
import * as v from './valita.ts';

const path: (string | number)[] = [];

export const jsonSchema: valita.Type<ReadonlyJSONValue> = v
  .unknown()
  .chain(v => {
    if (skipAssertJSONValue) {
      return valita.ok(v as ReadonlyJSONValue);
    }
    const rv = isJSONValue(v, path)
      ? valita.ok(v)
      : valita.err({
          message: `Not a JSON value`,
          path: path.slice(),
        });
    path.length = 0;
    return rv;
  });

export const jsonObjectSchema: valita.Type<ReadonlyJSONObject> = v
  .unknown()
  .chain(v => {
    if (skipAssertJSONValue) {
      return valita.ok(v as ReadonlyJSONObject);
    }
    const rv = isJSONObject(v, path)
      ? valita.ok(v)
      : valita.err({
          message: `Not a JSON object`,
          path: path.slice(),
        });
    path.length = 0;
    return rv;
  });
