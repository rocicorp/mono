import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  enumeration,
  json,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';

const jsonCols = {
  str: json<string>(),
  num: json<number>(),
  bool: json<boolean>(),
  nil: json<null>(),
  obj: json<{foo: string}>(),
  arr: json<string[]>(),
} as const;

export const schema = createSchema({
  tables: [
    table('basic')
      .columns({
        id: string(),
        a: number(),
        b: string(),
        c: boolean().optional(),
      })
      .primaryKey('id'),
    table('names')
      .from('divergent_names')
      .columns({
        id: string().from('divergent_id'),
        a: number().from('divergent_a'),
        b: string().from('divergent_b'),
        c: boolean().from('divergent_c').optional(),
      })
      .primaryKey('id'),
    table('compoundPk')
      .columns({
        a: string(),
        b: number(),
        c: string().optional(),
      })
      .primaryKey('a', 'b'),
    table('dateTypes')
      .columns({
        ts: number(),
        tstz: number(),
        tswtz: number(),
        tswotz: number(),
        d: number(),
      })
      .primaryKey('ts'),
    table('jsonCases')
      .columns({
        ...jsonCols,
        str: string(),
      })
      .primaryKey('str'),
    table('jsonbCases').columns(jsonCols).primaryKey('str'),
    table('uuidAndEnum')
      .columns({
        id: string(),
        // eslint-disable-next-line @typescript-eslint/naming-convention
        reference_id: string(),
        status: enumeration<'active' | 'inactive' | 'pending'>(),
        type: enumeration<'user' | 'system' | 'admin'>(),
      })
      .primaryKey('id'),
  ],
  relationships: [],
});

export const schemaSql = `CREATE TABLE basic (
  id TEXT PRIMARY KEY,
  a INTEGER,
  b TEXT,
  C BOOLEAN
);

CREATE TABLE divergent_names (
  divergent_id TEXT PRIMARY KEY,
  divergent_a INTEGER,
  divergent_b TEXT,
  divergent_c BOOLEAN
);

CREATE TABLE "compoundPk" (
  a TEXT,
  b INTEGER,
  c TEXT,
  PRIMARY KEY (a, b)
);

CREATE TABLE "dateTypes" (
  "ts" TIMESTAMP,
  "tstz" TIMESTAMPTZ,
  "tswtz" TIMESTAMP WITH TIME ZONE,
  "tswotz" TIMESTAMP WITHOUT TIME ZONE,
  "d" DATE,
  PRIMARY KEY ("ts")
);

CREATE TABLE "jsonbCases" (
  "str" JSONB,
  "num" JSONB,
  "bool" JSONB,
  "nil" JSONB,
  "obj" JSONB,
  "arr" JSONB,
  PRIMARY KEY ("str")
);

CREATE TABLE "jsonCases" (
  "str" TEXT,
  "num" JSON,
  "bool" JSON,
  "nil" JSON,
  "obj" JSON,
  "arr" JSON,
  PRIMARY KEY ("str")
);

CREATE TYPE status_enum AS ENUM ('active', 'inactive', 'pending');
CREATE TYPE type_enum AS ENUM ('user', 'system', 'admin');

CREATE TABLE "uuidAndEnum" (
  "id" UUID PRIMARY KEY,
  "reference_id" UUID NOT NULL,
  "status" status_enum NOT NULL,
  "type" type_enum NOT NULL
);
`;

export const seedDataSql = `
INSERT INTO basic (id, a, b, c) VALUES ('1', 2, 'foo', true);
INSERT INTO divergent_names (divergent_id, divergent_a, divergent_b, divergent_c) VALUES ('2', 3, 'bar', false);
INSERT INTO "compoundPk" (a, b, c) VALUES ('a', 1, 'c');
INSERT INTO "dateTypes" (ts, tstz, tswtz, tswotz) VALUES (
  '2021-01-01 00:00:01',
  '2022-02-02 00:00:02',
  '2023-03-03 00:00:03',
  '2024-04-04 00:00:04'
);
`;
