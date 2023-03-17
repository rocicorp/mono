import * as valita from '@badrap/valita';
import type {ReadonlyJSONValue} from 'shared/json.js';
import {isJSONValue} from 'shared/json.js';
import * as v from 'shared/valita.js';

const path: (string | number)[] = [];

export const jsonSchema: valita.Type<ReadonlyJSONValue> = v
  .unknown()
  .chain(v => {
    const rv = isJSONValue(v, path)
      ? valita.ok(v)
      : valita.err({
          message: `Not a JSON value`,
          path: path.slice(),
        });
    path.length = 0;
    return rv;
  });

/**
 * A JSON value that allows undefined values in objects.
 */
export type RelaxedJSONValue =
  | boolean
  | null
  | number
  | string
  | RelaxedJSONObject
  | RelaxedJSONArray;

type RelaxedJSONArray = ReadonlyArray<RelaxedJSONValue>;

type RelaxedJSONObject = {readonly [key: string]: RelaxedJSONValue | undefined};
