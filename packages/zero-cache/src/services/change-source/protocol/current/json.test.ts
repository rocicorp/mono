import type * as v from '@badrap/valita';
import {expectTypeOf, test} from 'vitest';
import type {
  JSONObject,
  jsonObjectSchema,
  JSONValue,
  jsonValueSchema,
} from './json.ts';

test('json schema types', () => {
  expectTypeOf<v.Infer<typeof jsonValueSchema>>().toEqualTypeOf<JSONValue>();
  expectTypeOf<v.Infer<typeof jsonObjectSchema>>().toEqualTypeOf<JSONObject>();
});
