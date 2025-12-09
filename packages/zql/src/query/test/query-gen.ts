import {type Faker} from '@faker-js/faker';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {Schema} from '../../../../zero-types/src/schema.ts';
import type {ServerSchema} from '../../../../zero-types/src/server-schema.ts';
import {getDataForType} from '../../../../zql-integration-tests/src/helpers/data-gen.ts';
import {NotImplementedError} from '../../error.ts';
import {asQueryInternals} from '../query-internals.ts';
import type {AnyQuery} from '../query.ts';
import {staticQuery} from '../static-query.ts';
import {randomValueForType, selectRandom, shuffle, type Rng} from './util.ts';
export type Dataset = {
  [table: string]: Row[];
};

export function generateQuery(
  schema: Schema,
  data: Dataset,
  rng: Rng,
  faker: Faker,
  serverSchema?: ServerSchema,
): AnyQuery {
  return augmentQuery(
    schema,
    data,
    rng,
    faker,
    staticQuery(schema, selectRandom(rng, Object.keys(schema.tables))),
    serverSchema,
    [],
  );
}

export function generateShrinkableQuery(
  schema: Schema,
  data: Dataset,
  rng: Rng,
  faker: Faker,
  serverSchema?: ServerSchema,
): [AnyQuery, Generation[]] {
  const generations: Generation[] = [];
  const q = augmentQuery(
    schema,
    data,
    rng,
    faker,
    staticQuery(schema, selectRandom(rng, Object.keys(schema.tables))),
    serverSchema,
    generations,
  );
  return [q, generations];
}

type Generation = AnyQuery;

const maxDepth = 6;
function augmentQuery(
  schema: Schema,
  data: Dataset,
  rng: Rng,
  faker: Faker,
  query: AnyQuery,
  serverSchema: ServerSchema | undefined,
  generations: Generation[],
  depth = 0,
  inExists = false,
): AnyQuery {
  if (depth > maxDepth) {
    return query;
  }
  generations.push(query);

  if (inExists) {
    // If we are in exists, adding:
    // - related
    // - limit
    // - order by
    // makes no sense.
    // TODO: fuzzer does not fuzz start!
    return addWhere(addExists(query));
  }

  return addLimit(addOrderBy(addWhere(addExists(addRelated(query)))));

  function addLimit(query: AnyQuery) {
    if (rng() < 0.2) {
      return query;
    }

    try {
      query = query.limit(Math.floor(rng() * 200));
      generations.push(query);
      return query;
    } catch (e) {
      // junction tables don't support limit yet
      if (e instanceof NotImplementedError) {
        return query;
      }
      throw e;
    }
  }

  function addOrderBy(query: AnyQuery) {
    const tableName = asQueryInternals(query).ast.table;
    const table = schema.tables[tableName];
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
        generations.push(query);
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

    const tableName = asQueryInternals(query).ast.table;
    const table = schema.tables[tableName];
    const columnNames = Object.keys(table.columns);
    for (let i = 0; i < numConditions; i++) {
      const tableData = data[tableName];
      const columnName = selectRandom(rng, columnNames);
      const column = table.columns[columnName];
      const operator = selectRandom(rng, operatorsByType[column.type]);
      if (!operator) {
        continue;
      }

      let detailedType: string | undefined;
      if (serverSchema) {
        const serverTable = schema.tables[tableName].serverName ?? tableName;
        const serverColumn =
          schema.tables[tableName].columns[columnName].serverName ?? columnName;

        detailedType = serverSchema[serverTable]?.[serverColumn]?.type;
      }

      const value =
        // TODO: all these constants should be tunable.
        rng() > 0.1 && tableData && tableData.length > 0
          ? selectRandom(rng, tableData)[columnName]
          : detailedType
            ? getDataForType(faker, rng, {
                optional: !!column.optional,
                pgType: detailedType,
                isEnum: false,
                isPrimaryKey: false,
                name: columnName,
              })
            : randomValueForType(rng, faker, column.type, column.optional);
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      query = query.where(columnName as any, operator, value);
      generations.push(query);
    }

    return query;
  }

  function addRelated(query: AnyQuery) {
    // the deeper we go, the less likely we are to add a related table
    if (rng() * maxDepth < depth / 1.5) {
      return query;
    }

    const tableName = asQueryInternals(query).ast.table;
    const relationships = Object.keys(schema.relationships[tableName] ?? {});
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
      const subGenerations: Generation[] = [];
      const origQuery = query;
      query = query.related(relationshipName, q =>
        augmentQuery(
          schema,
          data,
          rng,
          faker,
          q,
          serverSchema,
          subGenerations,
          depth + 1,
          inExists,
        ),
      );
      for (const q of subGenerations) {
        generations.push(origQuery.related(relationshipName, _ => q));
      }
    });

    return query;
  }

  function addExists(query: AnyQuery) {
    // the deeper we go, the less likely we are to add an exists check
    if (rng() * maxDepth < depth / 1.5) {
      return query;
    }

    const tableName = asQueryInternals(query).ast.table;
    const relationships = Object.keys(schema.relationships[tableName] ?? {});
    const existsToAdd = Math.floor(rng() * 4);
    if (existsToAdd === 0) {
      return query;
    }
    const shuffledRelationships = shuffle(rng, relationships);
    const existsToAddNames = shuffledRelationships.slice(0, existsToAdd);
    existsToAddNames.forEach(relationshipName => {
      if (rng() < 0.5) {
        const subGenerations: Generation[] = [];
        const origQuery = query;
        query = query.where(({not, exists}) =>
          not(
            exists(relationshipName, q =>
              augmentQuery(
                schema,
                data,
                rng,
                faker,
                q,
                serverSchema,
                subGenerations,
                depth + 1,
                true,
              ),
            ),
          ),
        );
        for (const q of subGenerations) {
          generations.push(
            origQuery.where(({not, exists}) =>
              not(exists(relationshipName, _ => q)),
            ),
          );
        }
      } else {
        const subGenerations: Generation[] = [];
        const origQuery = query;
        query = query.whereExists(
          relationshipName,
          q =>
            augmentQuery(
              schema,
              data,
              rng,
              faker,
              q,
              serverSchema,
              subGenerations,
              depth + 1,
              true,
            ),
          rng() < 0.5 ? {flip: true} : undefined,
        );
        for (const q of subGenerations) {
          generations.push(origQuery.whereExists(relationshipName, _ => q));
        }
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
