import * as valita from '@badrap/valita';
import {skipAssertJSONValue} from 'shared/config.js';
import type {ReadonlyJSONValue} from 'shared/json.js';
import {isJSONValue} from 'shared/json.js';
import * as v from 'shared/valita.js';

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
