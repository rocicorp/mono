import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';

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
);`;

export const seedDataSql = `
INSERT INTO basic (id, a, b, c) VALUES ('1', 2, 'foo', true);
INSERT INTO divergent_names (divergent_id, divergent_a, divergent_b, divergent_c) VALUES ('2', 3, 'bar', false);
INSERT INTO "compoundPk" (a, b, c) VALUES ('a', 1, 'c');
`;
