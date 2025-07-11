import type {Faker} from '@faker-js/faker';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {Rng} from '../../../zql/src/query/test/util.ts';
import type {ServerSchema} from '../../../zero-schema/src/server-schema.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';

export type Dataset = {
  [table: string]: Row[];
};

type TopLevelOptions = {
  numRows?: number;
  tables?: Record<string, TableOptions>;
};

type TableOptions = {
  numRows?: number;
};

type ColumnInfo = {
  nullable: boolean;
  pgType: string;
  isEnum: boolean;
  isPrimaryKey: boolean;
  // to be used in the future for smart relationship generation
  // isForeignKey: boolean;
  name: string;
};

/**
 *
 * Generates data that matches a ZQL schema.
 * Also takes in the serverSchema given that provides
 * more narrow types for columns.
 *
 * The output data is compatible with the server
 * schema but typed as zql data types.
 *
 * @param rng
 * @param faker
 * @param zqlSchema
 * @param serverSchema
 * @param options
 * @returns
 */
export function generateData(
  rng: Rng,
  faker: Faker,
  zqlSchema: Schema,
  serverSchema: ServerSchema,
  options: TopLevelOptions,
) {
  const ret: Dataset = {};
  const toServerNameMapper = clientToServer(zqlSchema.tables);

  const tables = Object.entries(zqlSchema.tables);
  const tableOptions = options.tables ?? {};

  for (const [tableName, tableSchema] of tables) {
    const serverTableName = toServerNameMapper.tableName(tableName);
    const tableOption = tableOptions[tableName] ?? {};
    const numRowsForTable =
      tableOption.numRows ?? options.numRows ?? Math.floor(rng() * 10) + 1;

    const columns = Object.entries(tableSchema.columns);
    const rows: Row[] = [];
    for (let i = 0; i < numRowsForTable; i++) {
      const row: Record<string, JSONValue> = {};
      for (const [columnName, zqlColumnSchema] of columns) {
        const serverColumnSchema =
          serverSchema[serverTableName][
            toServerNameMapper.columnName(tableName, columnName)
          ];
        row[columnName] = getDataForType(faker, rng, {
          isEnum: serverColumnSchema.isEnum,
          isPrimaryKey: tableSchema.primaryKey.includes(columnName),
          name: columnName,
          nullable: !!zqlColumnSchema.nullable,
          pgType: serverColumnSchema.type,
        });
      }
      rows.push(row);
    }
    ret[tableName] = rows;
  }

  return ret;
}

export function getDataForType(faker: Faker, rng: Rng, column: ColumnInfo) {
  if (column.nullable) {
    if (rng() < 0.1) {
      return null;
    }
  }

  // remove the length of the type e.g., char(10) -> char
  const type = column.pgType.replace(/\(.*\)/, '');
  switch (type) {
    case 'smallint':
    case 'int2':
      return faker.number.int({min: -32768, max: 32767});
    case 'integer':
    case 'int':
    case 'int4':
      return faker.number.int({min: -2147483648, max: 2147483647});
    case 'int8':
    case 'bigint':
      // TODO: we currently do not support out of range bigints in zero
      return faker.number.int({
        min: Number.MIN_SAFE_INTEGER,
        max: Number.MAX_SAFE_INTEGER,
      });
    // return faker.number.bigInt({
    //   min: -9223372036854775808n,
    //   max: 9223372036854775807n,
    // });
    case 'smallserial':
    case 'serial2':
      return faker.number.int({min: 1, max: 32767});
    case 'serial':
    case 'serial4':
      return faker.number.int({min: 1, max: 2147483647});
    case 'serial8':
    case 'bigserial':
      return faker.number.int({
        min: 1,
        max: Number.MAX_SAFE_INTEGER,
      });
    case 'decimal':
    case 'numeric':
    case 'real':
    case 'double precision':
    case 'float8':
      return faker.number.float({
        min: Number.MIN_VALUE,
        max: Number.MAX_VALUE,
      });
    case 'float':
    case 'float4':
      return faker.number.float({
        min: -3.4028235e38,
        max: 3.4028235e38,
      });
    case 'date':
    case 'timestamp':
    case 'timestamptz':
    case 'timestamp with time zone':
    case 'timestamp without time zone':
      return faker.date.anytime().getTime();
    case 'bpchar':
    case 'character':
    case 'character varying':
    case 'varchar':
      return faker.string.alphanumeric(10);
    case 'uuid':
      return faker.string.uuid();
    case 'text':
      return faker.lorem.paragraph();
    case 'bool':
    case 'boolean':
      return faker.datatype.boolean();
    case 'json':
    case 'jsonb':
      return {};
    default:
      throw new Error(
        `Unsupported type "${column.pgType}" for column "${column.name}"`,
      );
  }
}
