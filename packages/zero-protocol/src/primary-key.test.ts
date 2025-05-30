import {expectTypeOf, test} from 'vitest';
import type {PrimaryKey} from '../../zql/src/ivm/constraint.ts';
import * as v from '../../shared/src/valita.ts';
import {primaryKeySchema} from './primary-key.ts';

test('types', () => {
  expectTypeOf<PrimaryKey>().toEqualTypeOf<v.Infer<typeof primaryKeySchema>>();
});
