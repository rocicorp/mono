import {type Faker} from '@faker-js/faker';
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
import {NotImplementedError} from '../../error.ts';
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

const maxDepth = 10;
function augmentQuery(
  schema: Schema,
  data: Dataset,
  rng: Rng,
  faker: Faker,
  query: AnyQuery,
  depth = 0,
  inExists = false,
) {
  if (depth > maxDepth) {
    return query;
  }
  return addLimit(
    addOrderBy(
      addWhere(
        addExists(
          // If we are in exists, adding `related` makes no sense.
          inExists ? query : addRelated(query),
        ),
      ),
    ),
  );

  function addLimit(query: AnyQuery) {
    if (rng() < 0.2) {
      return query;
    }

    try {
      return query.limit(Math.floor(rng() * 200));
    } catch (e) {
      // junction tables don't support limit yet
      if (e instanceof NotImplementedError) {
        return query;
      }
      throw e;
    }
  }

  function addOrderBy(query: AnyQuery) {
    const table = schema.tables[ast(query).table];
    const columnNames = Object.keys(table.columns);
    // we wouldn't really order by _every_ column, right?
    const numCols = Math.floor((rng() * columnNames.length) / 2);
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
    try {
      columns.forEach(({name, direction}) => {
        query = query.orderBy(name, direction);
      });
    } catch (e) {
      // junction tables don't support order by yet
      if (e instanceof NotImplementedError) {
        return query;
      }
      throw e;
    }

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

  function addRelated(query: AnyQuery) {
    // the deeper we go, the less likely we are to add a related table
    if (rng() * maxDepth < depth / 1.5) {
      return query;
    }

    const relationships = Object.keys(schema.relationships[ast(query).table]);
    const relationshipsToAdd = Math.floor(rng() * 4);
    if (relationshipsToAdd === 0) {
      return query;
    }
    const shuffledRelationships = shuffle(rng, relationships);
    const relationshipsToAddNames = shuffledRelationships.slice(
      0,
      relationshipsToAdd,
    );
    relationshipsToAddNames.forEach(relationshipName => {
      query = query.related(relationshipName, q =>
        augmentQuery(schema, data, rng, faker, q, depth + 1, inExists),
      );
    });

    return query;
  }

  function addExists(query: AnyQuery) {
    // the deeper we go, the less likely we are to add an exists check
    if (rng() * maxDepth < depth / 1.5) {
      return query;
    }

    const relationships = Object.keys(schema.relationships[ast(query).table]);
    const existsToAdd = Math.floor(rng() * 4);
    if (existsToAdd === 0) {
      return query;
    }
    const shuffledRelationships = shuffle(rng, relationships);
    const existsToAddNames = shuffledRelationships.slice(0, existsToAdd);
    existsToAddNames.forEach(relationshipName => {
      if (rng() < 0.5) {
        query = query.where(({not, exists}) =>
          not(
            exists(relationshipName, q =>
              augmentQuery(schema, data, rng, faker, q, depth + 1, true),
            ),
          ),
        );
      } else {
        query = query.whereExists(relationshipName, q =>
          augmentQuery(schema, data, rng, faker, q, depth + 1, true),
        );
      }
    });

    return query;
  }
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
