import {describe, expect, expectTypeOf, test} from 'vitest';
import type {z} from 'zod';
import {
  boolean,
  enumeration,
  json,
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import type {SchemaValueToTSType} from '../../zero-types/src/schema-value.ts';
import type {TableSchema} from '../../zero-types/src/schema.ts';
import type {
  DeleteID,
  InsertValue,
  UpdateValue,
} from '../../zql/src/mutate/crud.ts';
import type {Row} from '../../zql/src/query/query.ts';
import {deleteSchema, insertSchema, rowSchema, updateSchema} from './zod.ts';

type Role = 'admin' | 'viewer';

const userTableBuilder = table('user')
  .columns({
    id: string(),
    name: string().optional(),
    age: number(),
    active: boolean(),
    role: enumeration<Role>(),
    meta: json().optional(),
  })
  .primaryKey('id');

const userTable = {
  ...userTableBuilder.build(),
  columns: {
    ...userTableBuilder.schema.columns,
    deletedAt: {type: 'null', optional: true},
  },
} as const satisfies TableSchema;

const membershipTable = table('membership')
  .columns({
    userID: string(),
    projectID: string(),
    role: enumeration<'member' | 'admin'>(),
  })
  .primaryKey('userID', 'projectID')
  .build();

describe('rowSchema', () => {
  test('validates rows and removes additional fields', () => {
    const schema = rowSchema(userTable);
    const parsed = schema.parse({
      id: 'u1',
      name: null,
      age: 30,
      active: true,
      role: 'admin',
      deletedAt: null,
      meta: {active: true},
      extra: 'removed',
    });

    expect(parsed).toMatchObject({
      id: 'u1',
      name: null,
      age: 30,
      active: true,
      role: 'admin',
      deletedAt: null,
      meta: {active: true},
    });
    expect(
      schema.parse({
        id: 'u1',
        age: 30,
        active: false,
        role: 'viewer',
        deletedAt: null,
        meta: {active: true},
      }),
    ).toStrictEqual({
      id: 'u1',
      age: 30,
      active: false,
      role: 'viewer',
      deletedAt: null,
      meta: {active: true},
    });
    expect(() =>
      schema.parse({id: 'u1', active: true, role: 'admin', deletedAt: null}),
    ).toThrowErrorMatchingInlineSnapshot(`
      [ZodError: [
        {
          "expected": "number",
          "code": "invalid_type",
          "path": [
            "age"
          ],
          "message": "Invalid input: expected number, received undefined"
        }
      ]]
    `);
  });
});

describe('insertSchema', () => {
  test('enforces required and optional fields', () => {
    const schema = insertSchema(userTable);

    expect(
      schema.parse({
        id: 'u1',
        age: 30,
        active: true,
        role: 'viewer',
        deletedAt: null,
      }),
    ).toEqual({
      id: 'u1',
      age: 30,
      active: true,
      role: 'viewer',
      deletedAt: null,
    });
    expect(
      schema.parse({
        id: 'u2',
        age: 31,
        active: false,
        role: 'admin',
        meta: {settings: []},
        name: undefined,
        deletedAt: null,
      }),
    ).toEqual({
      id: 'u2',
      age: 31,
      active: false,
      role: 'admin',
      meta: {settings: []},
      name: undefined,
      deletedAt: null,
    });
    expect(
      schema.parse({
        id: 'u1',
        age: 29,
        active: true,
        role: 'viewer',
        deletedAt: null,
        extra: 'nope',
      }),
    ).toEqual({
      id: 'u1',
      age: 29,
      active: true,
      role: 'viewer',
      deletedAt: null,
    });
    expect(() =>
      schema.parse({age: 30, active: true, role: 'admin', deletedAt: null}),
    ).toThrowErrorMatchingInlineSnapshot(`
      [ZodError: [
        {
          "expected": "string",
          "code": "invalid_type",
          "path": [
            "id"
          ],
          "message": "Invalid input: expected string, received undefined"
        }
      ]]
    `);
  });

  test('accepts complex JSON structures', () => {
    const schema = insertSchema(userTable);
    const value = schema.parse({
      id: 'u5',
      age: 25,
      active: true,
      role: 'viewer',
      deletedAt: null,
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
    expect(
      schema.parse({
        id: 'u1',
        name: null,
        active: false,
        role: 'viewer',
        deletedAt: null,
      }),
    ).toEqual({
      id: 'u1',
      name: null,
      active: false,
      role: 'viewer',
      deletedAt: null,
    });
    expect(schema.parse({id: 'u1', extra: 'nope'})).toEqual({
      id: 'u1',
    });
    expect(() => schema.parse({name: 'missing id'}))
      .toThrowErrorMatchingInlineSnapshot(`
        [ZodError: [
          {
            "expected": "string",
            "code": "invalid_type",
            "path": [
              "id"
            ],
            "message": "Invalid input: expected string, received undefined"
          }
        ]]
      `);
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
          "expected": "string",
          "code": "invalid_type",
          "path": [
            "projectID"
          ],
          "message": "Invalid input: expected string, received undefined"
        }
      ]]
    `);
  });
});

type Mutable<T> = {
  -readonly [K in keyof T]: T[K];
};

describe('type mappings', () => {
  test('rowSchema returns row', () => {
    type Output = z.output<ReturnType<typeof rowSchema<typeof userTable>>>;
    expectTypeOf<Output>().toEqualTypeOf<Mutable<Row<typeof userTable>>>();
    expectTypeOf<Output['name']>().toEqualTypeOf<string | null>();
    expectTypeOf<Output['meta']>().toEqualTypeOf<
      SchemaValueToTSType<typeof userTable.columns.meta>
    >();
    expectTypeOf<Output['active']>().toEqualTypeOf<boolean>();
    expectTypeOf<Output['role']>().toEqualTypeOf<Role>();
    expectTypeOf<Output['deletedAt']>().toEqualTypeOf<null>();
  });

  test('insertSchema matches InsertValue', () => {
    type Output = z.infer<ReturnType<typeof insertSchema<typeof userTable>>>;
    expectTypeOf<Output>().toEqualTypeOf<
      Mutable<InsertValue<typeof userTable>>
    >();
    expectTypeOf<Output['id']>().toEqualTypeOf<string>();
    expectTypeOf<Output['age']>().toEqualTypeOf<number>();
    expectTypeOf<Output['name']>().toEqualTypeOf<string | null | undefined>();
    expectTypeOf<Output['active']>().toEqualTypeOf<boolean>();
    expectTypeOf<Output['role']>().toEqualTypeOf<Role>();
    expectTypeOf<Output['deletedAt']>().toEqualTypeOf<null | undefined>();
  });

  test('updateSchema matches UpdateValue', () => {
    type Output = z.infer<ReturnType<typeof updateSchema<typeof userTable>>>;
    type Expected = Mutable<UpdateValue<typeof userTable>>;
    expectTypeOf<Output>().toEqualTypeOf<Expected>();
    expectTypeOf<Output['id']>().toEqualTypeOf<string>();
    expectTypeOf<Output['name']>().toEqualTypeOf<string | null | undefined>();
    expectTypeOf<Output['meta']>().toEqualTypeOf<
      SchemaValueToTSType<typeof userTable.columns.meta> | undefined
    >();
    expectTypeOf<Output['active']>().toEqualTypeOf<
      boolean | null | undefined
    >();
    expectTypeOf<Output['role']>().toEqualTypeOf<Role | null | undefined>();
    expectTypeOf<Output['deletedAt']>().toEqualTypeOf<null | undefined>();
  });

  test('deleteSchema matches primary key shape', () => {
    type Output = z.infer<
      ReturnType<typeof deleteSchema<typeof membershipTable>>
    >;
    expectTypeOf<Output>().toEqualTypeOf<
      Mutable<DeleteID<typeof membershipTable>>
    >();
    expectTypeOf<Output['userID']>().toEqualTypeOf<string>();
    expectTypeOf<Output['projectID']>().toEqualTypeOf<string>();
  });
});
