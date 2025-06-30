import type {Faker} from '@faker-js/faker';
import type {Schema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import type {
  Relationship,
  RelationshipsSchema,
  SchemaValue,
  TableSchema,
} from '../../../../zero-schema/src/table-schema.ts';
import {generateUniqueValues, selectRandom, shuffle, type Rng} from './util.ts';

const dbTypes = {
  string: ['text', 'varchar', 'char'],
  number: ['int', 'bigint', 'decimal'],
  boolean: ['bool'],
  json: ['jsonb', 'json'],
} as const;

export function generateSchema(
  rng: Rng,
  faker: Faker,
  numTables?: number,
): Schema {
  const tables = generateUniqueValues(
    faker.word.noun,
    numTables ?? Math.floor(rng() * 10) + 1,
  ).map(name => generateTable(name, rng, faker));
  const relationships = generateRelationships(rng, tables);

  return {
    tables: Object.fromEntries(tables.map(table => [table.name, table])),
    relationships,
  };
}

function generateTable(name: string, rng: Rng, faker: Faker): TableSchema {
  const columns = generateUniqueValues(
    faker.word.noun,
    Math.floor(rng() * 10) + 1,
  ).map(name => generateColumn(name, rng, faker));
  const numPkColumns = Math.min(rng() < 0.5 ? 1 : 2, columns.length);

  return {
    name,
    columns: Object.fromEntries(columns),
    primaryKey:
      numPkColumns === 1 ? [columns[0][0]] : [columns[0][0], columns[1][0]],
  };
}

function generateRelationships(
  rng: Rng,
  tables: TableSchema[],
): {
  [table: string]: RelationshipsSchema;
} {
  const relationships: {[table: string]: RelationshipsSchema} = {};
  for (const table of tables) {
    relationships[table.name] = generateRelationshipsForTable(
      rng,
      table,
      tables,
    );
  }
  return relationships;
}

function generateRelationshipsForTable(
  rng: Rng,
  table: TableSchema,
  tables: TableSchema[],
): RelationshipsSchema {
  const numRelationships = Math.floor(rng() * 3);
  if (numRelationships === 0) {
    return {};
  }

  const shuffledTables = shuffle(rng, tables);

  const relationships: Record<string, Relationship> = {};
  for (let i = 0; i < numRelationships; i++) {
    const destTable = shuffledTables[i];
    const sourceField = selectRandom(rng, Object.keys(table.columns));
    if (table.columns[sourceField].type === 'json') {
      continue;
    }

    const destTargets = Object.entries(destTable.columns).filter(
      ([_, column]) => column.type === table.columns[sourceField].type,
    );
    if (destTargets.length === 0) {
      continue;
    }

    const destField = selectRandom(rng, destTargets);
    const cardinality =
      destField[1].nullable || destField[1].type === 'boolean'
        ? 'many'
        : selectRandom(rng, ['one', 'many'] as const);

    relationships[destTable.name] = [
      {
        sourceField: [sourceField],
        destField: [destField[0]],
        destSchema: destTable.name,
        cardinality,
      },
    ];
  }

  return relationships;
}

function generateColumn(
  name: string,
  rng: Rng,
  faker: Faker,
): [string, SchemaValue] {
  const type = selectRandom(rng, [
    'string',
    'string',
    'string',
    'string',
    'string',
    'boolean',
    'number',
    'number',
    'number',
    'json',
  ]) as keyof typeof dbTypes;

  const generateDefault =
    type === 'string'
      ? () => faker.lorem.word()
      : type === 'number'
        ? () => faker.number.int()
        : type === 'boolean'
          ? () => faker.datatype.boolean()
          : type === 'json'
            ? () =>
                faker.helpers.arrayElement([
                  {uno: faker.lorem.word()},
                  {dos: faker.lorem.word()},
                  {tres: faker.lorem.word()},
                ])
            : null;

  return [
    name,
    {
      type,
      nullable: rng() < 0.5,
      ...(rng() < 0.7 && generateDefault
        ? {
            defaultConfig: {
              insert: {
                client: generateDefault,
                server: rng() < 0.5 ? 'db' : generateDefault,
              },
              update: {
                client: generateDefault,
                server: rng() < 0.5 ? 'db' : generateDefault,
              },
            },
          }
        : {}),
    },
  ];
}
