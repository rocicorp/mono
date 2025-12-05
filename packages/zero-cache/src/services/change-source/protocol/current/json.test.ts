import * as v from '@badrap/valita';
import {expectTypeOf, test} from 'vitest';
import {
  jsonObjectSchema,
  jsonValueSchema,
  type JSONObject,
  type JSONValue,
} from './json.ts';

test('json schema types', () => {
  expectTypeOf<v.Infer<typeof jsonValueSchema>>().toEqualTypeOf<JSONValue>();
  expectTypeOf<v.Infer<typeof jsonObjectSchema>>().toEqualTypeOf<JSONObject>();
});
