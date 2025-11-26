import {expect, test} from 'vitest';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';
import {bindingsForZero} from './bindings.ts';
import {zeroForTest} from './test-utils.ts';

test('multiple metadata() calls return consistent results', () => {
  const z = zeroForTest({
    logLevel: 'debug',
    schema,
  });

  const zql = createBuilder(schema);

  const bindings = bindingsForZero(z);
  const query = zql.issue;

  // Call metadata multiple times
  const hash1 = bindings.hash(query);
  const hash2 = bindings.hash(query);
  const hash3 = bindings.hash(query);
  expect(hash1).toBe(hash2);
  expect(hash2).toBe(hash3);

  const format1 = bindings.format(query);
  const format2 = bindings.format(query);
  const format3 = bindings.format(query);
  expect(format1).toBe(format2);
  expect(format2).toBe(format3);
});
