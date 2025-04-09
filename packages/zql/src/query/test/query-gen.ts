import type {Faker} from '@faker-js/faker';
import type {Schema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {ast} from '../query-impl.ts';
import {staticQuery} from '../static-query.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {
  randomValueForType,
  selectRandom,
  shuffle,
  type AnyQuery,
  type Rng,
} from './util.ts';
export type Dataset = {
  [table: string]: Row[];
};

export function generateQuery(
  schema: Schema,
  data: Dataset,
  rng: Rng,
  faker: Faker,
): AnyQuery {
  const rootTable = selectRandom(rng, Object.keys(schema.tables));
  return augmentQuery(schema, data, rng, faker, staticQuery(schema, rootTable));
}

function augmentQuery(
  schema: Schema,
  data: Dataset,
  rng: Rng,
  faker: Faker,
  query: AnyQuery,
) {
  return addLimit(addOrderBy(addWhere(query)));

  function addLimit(query: AnyQuery) {
    if (rng() < 0.5) {
      return query;
    }

    return query.limit(Math.floor(rng() * 10_000));
  }

  function addOrderBy(query: AnyQuery) {
    const table = schema.tables[ast(query).table];
    const columnNames = Object.keys(table.columns);
    const numCols = Math.floor(rng() * columnNames.length);
    if (numCols === 0) {
      return query;
    }

    const shuffledColumns = shuffle(rng, columnNames);
    const columns = shuffledColumns.slice(0, numCols).map(
      name =>
        ({
          name,
          direction: rng() < 0.5 ? 'asc' : 'desc',
        }) as const,
    );
    columns.forEach(({name, direction}) => {
      query = query.orderBy(name, direction);
    });

    return query;
  }

  function addWhere(query: AnyQuery) {
    const numConditions = Math.floor(rng() * 5);
    if (numConditions === 0) {
      return query;
    }

    const table = schema.tables[ast(query).table];
    const columnNames = Object.keys(table.columns);
    for (let i = 0; i < numConditions; i++) {
      const tableData = data[ast(query).table];
      const columnName = selectRandom(rng, columnNames);
      const column = table.columns[columnName];
      const operator = selectRandom(rng, operatorsByType[column.type]);
      if (!operator) {
        continue;
      }
      const value =
        // TODO: all these constants should be tunable.
        rng() > 0.1 && tableData && tableData.length > 0
          ? selectRandom(rng, tableData)[columnName]
          : randomValueForType(rng, faker, column.type, column.optional);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      query = query.where(columnName as any, operator, value);
    }

    return query;
  }

  // addRelated
  // addExists
}

const operatorsByType = {
  // we don't support not like?????
  string: ['=', '!=', 'IS', 'IS NOT', 'LIKE', 'ILIKE'],
  boolean: ['=', '!=', 'IS', 'IS NOT'],
  number: ['=', '<', '>', '<=', '>=', '!=', 'IS', 'IS NOT'],
  date: ['=', '<', '>', '<=', '>=', '!=', 'IS', 'IS NOT'],
  timestamp: ['=', '<', '>', '<=', '>=', '!=', 'IS', 'IS NOT'],
  // not comparable in our system yet
  json: [],
  null: ['IS', 'IS NOT'],
} as const;
