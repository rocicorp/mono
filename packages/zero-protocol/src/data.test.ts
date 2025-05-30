import {expectTypeOf, test} from 'vitest';
import * as v from '../../shared/src/valita.ts';
import {rowSchema, valueSchema} from './data.ts';
import type {Value, Row} from '../../zql/src/ivm/data.ts';

test('types', () => {
  expectTypeOf<v.Infer<typeof valueSchema>>().toEqualTypeOf<Value>();
  expectTypeOf<v.Infer<typeof rowSchema>>().toEqualTypeOf<Row>();
});
