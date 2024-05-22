import type {ReadonlyJSONObject} from 'shared/src/json.js';

export type Entity = {
  readonly id: string;
} & ReadonlyJSONObject;
