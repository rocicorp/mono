import {describe, expect, expectTypeOf, test} from 'vitest';
import type {z} from 'zod';
import type {SchemaValueToTSType} from '../../zero-types/src/schema-value.ts';
import type {TableSchema} from '../../zero-types/src/schema.ts';
import type {
  DeleteID,
  InsertValue,
  UpdateValue,
} from '../../zql/src/mutate/crud.ts';
import type {Row} from '../../zql/src/query/query.ts';
import {deleteSchema, insertSchema, rowSchema, updateSchema} from './zod.ts';

const userTable = {
  name: 'user',
  columns: {
    id: {type: 'string'},
    name: {type: 'string', optional: true},
    age: {type: 'number'},
    meta: {type: 'json', optional: true},
  },
  primaryKey: ['id'],
} as const satisfies TableSchema;

const membershipTable = {
  name: 'membership',
  columns: {
    userID: {type: 'string'},
    projectID: {type: 'string'},
    role: {type: 'string'},
  },
  primaryKey: ['userID', 'projectID'],
} as const satisfies TableSchema;

describe('rowSchema', () => {
  test('validates rows and removes additional fields', () => {
    const schema = rowSchema(userTable);
    const parsed = schema.parse({
      id: 'u1',
      name: null,
      age: 30,
      meta: {active: true},
      extra: 'removed',
    });

    expect(parsed).toMatchObject({
      id: 'u1',
      name: null,
      age: 30,
      meta: {active: true},
    });
    expect(() => schema.parse({id: 'u1', age: 30, meta: {active: true}}))
      .toThrowErrorMatchingInlineSnapshot(`
      [ZodError: [
        {
          "code": "invalid_type",
          "expected": "string",
          "received": "undefined",
          "path": [
            "name"
          ],
          "message": "Required"
        }
      ]]
    `);
  });
});

describe('insertSchema', () => {
  test('enforces required and optional fields', () => {
    const schema = insertSchema(userTable);

    expect(schema.parse({id: 'u1', age: 30})).toEqual({id: 'u1', age: 30});
    expect(
      schema.parse({id: 'u2', age: 31, meta: {settings: []}, name: undefined}),
    ).toEqual({id: 'u2', age: 31, meta: {settings: []}, name: undefined});
    expect(schema.parse({id: 'u1', age: 29, extra: 'nope'})).toEqual({
      id: 'u1',
      age: 29,
    });
    expect(() => schema.parse({age: 30})).toThrowErrorMatchingInlineSnapshot(`
      [ZodError: [
        {
          "code": "invalid_type",
          "expected": "string",
          "received": "undefined",
          "path": [
            "id"
          ],
          "message": "Required"
        }
      ]]
    `);
  });

  test('rejects non-JSON meta', () => {
    const schema = insertSchema(userTable);
    expect(() =>
      schema.parse({
        id: 'u4',
        age: 40,
        // Functions are not valid JSON values
        meta: {invalid: () => 'nope'},
      }),
    ).toThrowError(/JSON/);
  });

  test('accepts complex JSON structures', () => {
    const schema = insertSchema(userTable);
    const value = schema.parse({
      id: 'u5',
      age: 25,
      meta: {nested: [{a: 1}, ['b', null], {c: false}]},
    });
    expect(value.meta).toEqual({nested: [{a: 1}, ['b', null], {c: false}]});
  });
});

describe('updateSchema', () => {
  test('requires primary key and allows partial updates', () => {
    const schema = updateSchema(userTable);

    expect(schema.parse({id: 'u1'})).toEqual({id: 'u1'});
    expect(schema.parse({id: 'u1', name: 'New Name'})).toEqual({
      id: 'u1',
      name: 'New Name',
    });
    expect(schema.parse({id: 'u1', name: null})).toEqual({
      id: 'u1',
      name: null,
    });
    expect(schema.parse({id: 'u1', extra: 'nope'})).toEqual({
      id: 'u1',
    });
    expect(() => schema.parse({name: 'missing id'}))
      .toThrowErrorMatchingInlineSnapshot(`
      [ZodError: [
        {
          "code": "invalid_type",
          "expected": "string",
          "received": "undefined",
          "path": [
            "id"
          ],
          "message": "Required"
        }
      ]]
    `);
  });

  test('rejects non-JSON updates', () => {
    const schema = updateSchema(userTable);
    expect(() => schema.parse({id: 'u1', meta: () => 'nope'})).toThrowError(
      /JSON/,
    );
  });
});

describe('deleteSchema', () => {
  test('requires every primary key column', () => {
    const schema = deleteSchema(membershipTable);

    expect(schema.parse({userID: 'u1', projectID: 'p1'})).toStrictEqual({
      userID: 'u1',
      projectID: 'p1',
    });
    // strips extra fields
    expect(
      schema.parse({userID: 'u1', projectID: 'p1', role: 'admin'}),
    ).toStrictEqual({
      userID: 'u1',
      projectID: 'p1',
    });

    expect(() => schema.parse({userID: 'u1'}))
      .toThrowErrorMatchingInlineSnapshot(`
      [ZodError: [
        {
          "code": "invalid_type",
          "expected": "string",
          "received": "undefined",
          "path": [
            "projectID"
          ],
          "message": "Required"
        }
      ]]
    `);
  });
});

describe('type mappings', () => {
  test('rowSchema returns row', () => {
    type Output = z.output<ReturnType<typeof rowSchema<typeof userTable>>>;
    expectTypeOf<Output>().toEqualTypeOf<Row<typeof userTable>>();
    expectTypeOf<Output['name']>().toEqualTypeOf<string | null>();
    expectTypeOf<Output['meta']>().toEqualTypeOf<
      SchemaValueToTSType<typeof userTable.columns.meta>
    >();
  });

  test('insertSchema matches InsertValue', () => {
    type Output = z.input<ReturnType<typeof insertSchema<typeof userTable>>>;
    expectTypeOf<Output>().toEqualTypeOf<InsertValue<typeof userTable>>();
    expectTypeOf<Output['id']>().toEqualTypeOf<string>();
    expectTypeOf<Output['age']>().toEqualTypeOf<number>();
    expectTypeOf<Output['name']>().toEqualTypeOf<string | null | undefined>();
  });

  test('updateSchema matches UpdateValue', () => {
    type Output = z.input<ReturnType<typeof updateSchema<typeof userTable>>>;
    expectTypeOf<Output>().toEqualTypeOf<UpdateValue<typeof userTable>>();
    expectTypeOf<Output['id']>().toEqualTypeOf<string>();
    expectTypeOf<Output['name']>().toEqualTypeOf<string | null | undefined>();
    expectTypeOf<Output['meta']>().toEqualTypeOf<
      SchemaValueToTSType<typeof userTable.columns.meta> | undefined
    >();
  });

  test('deleteSchema matches primary key shape', () => {
    type Output = z.input<
      ReturnType<typeof deleteSchema<typeof membershipTable>>
    >;
    expectTypeOf<Output>().toEqualTypeOf<DeleteID<typeof membershipTable>>();
    expectTypeOf<Output['userID']>().toEqualTypeOf<string>();
    expectTypeOf<Output['projectID']>().toEqualTypeOf<string>();
  });
});
