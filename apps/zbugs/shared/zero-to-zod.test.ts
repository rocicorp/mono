import {expect, expectTypeOf, test} from 'vitest';
import type {z} from 'zod/mini';
import {
  boolean,
  json,
  number,
  string,
  table,
} from '../../../packages/zero-schema/src/builder/table-builder.ts';
import {zeroToZod} from './zero-to-zod.ts';

test('zeroToZod converts string columns', () => {
  const testTable = table('test')
    .columns({
      id: string(),
      name: string(),
    })
    .primaryKey('id');

  const zodSchema = zeroToZod(testTable.schema);

  const result = zodSchema.safeParse({id: '123', name: 'John'});
  expect(result.success).toBe(true);
  expect(result.success && result.data).toEqual({id: '123', name: 'John'});

  const invalidResult = zodSchema.safeParse({id: 123, name: 'John'});
  expect(invalidResult.success).toBe(false);
});

test('zeroToZod converts number columns', () => {
  const testTable = table('test')
    .columns({
      id: string(),
      count: number(),
      score: number(),
    })
    .primaryKey('id');

  const zodSchema = zeroToZod(testTable.schema);

  const result = zodSchema.safeParse({id: 'x', count: 5, score: 3.14});
  expect(result.success).toBe(true);
  expect(result.success && result.data).toEqual({
    id: 'x',
    count: 5,
    score: 3.14,
  });

  const invalidResult = zodSchema.safeParse({id: 'x', count: '5', score: 3.14});
  expect(invalidResult.success).toBe(false);
});

test('zeroToZod converts boolean columns', () => {
  const testTable = table('test')
    .columns({
      id: string(),
      active: boolean(),
      verified: boolean(),
    })
    .primaryKey('id');

  const zodSchema = zeroToZod(testTable.schema);

  const result = zodSchema.safeParse({id: 'x', active: true, verified: false});
  expect(result.success).toBe(true);
  expect(result.success && result.data).toEqual({
    id: 'x',
    active: true,
    verified: false,
  });

  const invalidResult = zodSchema.safeParse({
    id: 'x',
    active: 'true',
    verified: false,
  });
  expect(invalidResult.success).toBe(false);
});

test('zeroToZod converts optional columns', () => {
  const testTable = table('test')
    .columns({
      id: string(),
      name: string().optional(),
      count: number().optional(),
    })
    .primaryKey('id');

  const zodSchema = zeroToZod(testTable.schema);

  // With optional fields present
  const result1 = zodSchema.safeParse({id: 'x', name: 'John', count: 5});
  expect(result1.success).toBe(true);

  // Without optional fields
  const result2 = zodSchema.safeParse({id: 'x'});
  expect(result2.success).toBe(true);

  // With undefined optional fields
  const result3 = zodSchema.safeParse({
    id: 'x',
    name: undefined,
    count: undefined,
  });
  expect(result3.success).toBe(true);
});

test('zeroToZod converts json columns', () => {
  const testTable = table('test')
    .columns({
      id: string(),
      metadata: json(),
    })
    .primaryKey('id');

  const zodSchema = zeroToZod(testTable.schema);

  const result = zodSchema.safeParse({
    id: 'x',
    metadata: {key: 'value', nested: {array: [1, 2, 3]}},
  });
  expect(result.success).toBe(true);
});

test('zeroToZod converts mixed column types', () => {
  const testTable = table('test')
    .columns({
      id: string(),
      name: string().optional(),
      age: number(),
      active: boolean(),
      metadata: json().optional(),
    })
    .primaryKey('id');

  const zodSchema = zeroToZod(testTable.schema);

  const result = zodSchema.safeParse({
    id: '123',
    name: 'Alice',
    age: 30,
    active: true,
    metadata: {role: 'admin'},
  });
  expect(result.success).toBe(true);

  const resultWithoutOptional = zodSchema.safeParse({
    id: '123',
    age: 30,
    active: true,
  });
  expect(resultWithoutOptional.success).toBe(true);
});

test('zeroToZod type inference works correctly', () => {
  const testTable = table('test')
    .columns({
      id: string(),
      name: string(),
      age: number().optional(),
    })
    .primaryKey('id');

  const zodSchema = zeroToZod(testTable.schema);
  type InferredType = z.infer<typeof zodSchema>;

  // Type assertion to verify the inferred type structure
  const data: InferredType = {
    id: '123',
    name: 'John',
    age: 25,
  };

  expect(zodSchema.safeParse(data).success).toBe(true);

  // Optional field can be omitted
  const dataWithoutOptional: InferredType = {
    id: '456',
    name: 'Jane',
  };

  expect(zodSchema.safeParse(dataWithoutOptional).success).toBe(true);
});

test('zeroToZod rejects invalid data types', () => {
  const testTable = table('test')
    .columns({
      id: string(),
      count: number(),
      active: boolean(),
    })
    .primaryKey('id');

  const zodSchema = zeroToZod(testTable.schema);

  // Wrong types
  expect(zodSchema.safeParse({id: 123, count: 5, active: true}).success).toBe(
    false,
  );
  expect(zodSchema.safeParse({id: 'x', count: '5', active: true}).success).toBe(
    false,
  );
  expect(zodSchema.safeParse({id: 'x', count: 5, active: 'true'}).success).toBe(
    false,
  );

  // Missing required fields
  expect(zodSchema.safeParse({id: 'x', count: 5}).success).toBe(false);
  expect(zodSchema.safeParse({id: 'x'}).success).toBe(false);
  expect(zodSchema.safeParse({}).success).toBe(false);
});

// Type tests - these verify compile-time type checking
test('type: inferred types match Zero table schema', () => {
  const testTable = table('test')
    .columns({
      id: string(),
      name: string(),
      age: number(),
      active: boolean(),
    })
    .primaryKey('id');

  const zodSchema = zeroToZod(testTable.schema);
  type InferredType = z.infer<typeof zodSchema>;

  // Verify correct types
  expectTypeOf<InferredType>().toEqualTypeOf<{
    id: string;
    name: string;
    age: number;
    active: boolean;
  }>();

  // Verify field types individually
  expectTypeOf<InferredType['id']>().toEqualTypeOf<string>();
  expectTypeOf<InferredType['name']>().toEqualTypeOf<string>();
  expectTypeOf<InferredType['age']>().toEqualTypeOf<number>();
  expectTypeOf<InferredType['active']>().toEqualTypeOf<boolean>();

  // Verify runtime validation works
  const validData: InferredType = {
    id: 'abc',
    name: 'Alice',
    age: 30,
    active: true,
  };

  expect(zodSchema.safeParse(validData).success).toBe(true);
  expect(
    zodSchema.safeParse({id: 123, name: 'Alice', age: 30, active: true})
      .success,
  ).toBe(false);
  expect(
    zodSchema.safeParse({id: 'abc', name: 123, age: 30, active: true}).success,
  ).toBe(false);
  expect(
    zodSchema.safeParse({id: 'abc', name: 'Alice', age: '30', active: true})
      .success,
  ).toBe(false);
  expect(
    zodSchema.safeParse({id: 'abc', name: 'Alice', age: 30, active: 'true'})
      .success,
  ).toBe(false);
  expect(zodSchema.safeParse({id: 'abc', age: 30, active: true}).success).toBe(
    false,
  );
});

test('type: optional fields are properly typed', () => {
  const testTable = table('test')
    .columns({
      id: string(),
      name: string().optional(),
      age: number().optional(),
    })
    .primaryKey('id');

  const zodSchema = zeroToZod(testTable.schema);
  type InferredType = z.infer<typeof zodSchema>;

  // Verify correct types
  expectTypeOf<InferredType>().toEqualTypeOf<{
    id: string;
    name?: string | undefined;
    age?: number | undefined;
  }>();

  // Verify optional field types
  expectTypeOf<InferredType['id']>().toEqualTypeOf<string>();
  expectTypeOf<InferredType['name']>().toEqualTypeOf<string | undefined>();
  expectTypeOf<InferredType['age']>().toEqualTypeOf<number | undefined>();

  // Verify runtime behavior
  const onlyRequired: InferredType = {id: 'abc'};
  const withOneName: InferredType = {id: 'abc', name: 'Alice'};
  const withAll: InferredType = {id: 'abc', name: 'Alice', age: 30};
  const withUndefined: InferredType = {
    id: 'abc',
    name: undefined,
    age: undefined,
  };

  expect(zodSchema.safeParse(onlyRequired).success).toBe(true);
  expect(zodSchema.safeParse(withOneName).success).toBe(true);
  expect(zodSchema.safeParse(withAll).success).toBe(true);
  expect(zodSchema.safeParse(withUndefined).success).toBe(true);
  expect(zodSchema.safeParse({id: 'abc', name: 123}).success).toBe(false);
});

test('type: all column types are correctly inferred', () => {
  const testTable = table('test')
    .columns({
      id: string(),
      count: number(),
      active: boolean(),
      data: json(),
    })
    .primaryKey('id');

  const zodSchema = zeroToZod(testTable.schema);
  type InferredType = z.infer<typeof zodSchema>;

  // Verify correct types
  expectTypeOf<InferredType>().toMatchTypeOf<{
    id: string;
    count: number;
    active: boolean;
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    data: any;
  }>();

  // Verify individual field types
  expectTypeOf<InferredType['id']>().toEqualTypeOf<string>();
  expectTypeOf<InferredType['count']>().toEqualTypeOf<number>();
  expectTypeOf<InferredType['active']>().toEqualTypeOf<boolean>();
  expectTypeOf<InferredType['data']>().toBeAny();

  // Verify runtime behavior
  const validData: InferredType = {
    id: 'abc',
    count: 42,
    active: true,
    data: {nested: {value: 'test'}, array: [1, 2, 3]},
  };

  expect(zodSchema.safeParse(validData).success).toBe(true);
  expect(
    zodSchema.safeParse({id: 123, count: 42, active: true, data: {}}).success,
  ).toBe(false);
  expect(
    zodSchema.safeParse({id: 'abc', count: '42', active: true, data: {}})
      .success,
  ).toBe(false);
  expect(
    zodSchema.safeParse({id: 'abc', count: 42, active: 'true', data: {}})
      .success,
  ).toBe(false);
});
