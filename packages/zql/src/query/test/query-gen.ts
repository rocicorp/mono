import {generateMersenne53Randomizer, type Faker} from '@faker-js/faker';
import type {
  AST,
  CompoundKey,
  Condition,
  CorrelatedSubquery,
  LiteralValue,
  Ordering,
  SimpleOperator,
  System,
} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {Format} from '../../../../zero-types/src/format.ts';
import type {Relationship, Schema} from '../../../../zero-types/src/schema.ts';
import type {ServerSchema} from '../../../../zero-types/src/server-schema.ts';
import {getDataForType} from '../../../../zql-integration-tests/src/helpers/data-gen.ts';
import {NotImplementedError} from '../../error.ts';
import {newQueryImpl} from '../query-impl.ts';
import {asQueryInternals} from '../query-internals.ts';
import type {AnyQuery, ExistsOptions} from '../query.ts';
import {newStaticQuery} from '../static-query.ts';
import {randomValueForType, selectRandom, shuffle, type Rng} from './util.ts';
export type Dataset = {
  [table: string]: readonly Row[];
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
    newStaticQuery(schema, selectRandom(rng, Object.keys(schema.tables))),
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
    newStaticQuery(schema, selectRandom(rng, Object.keys(schema.tables))),
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
    const relationships = relationshipInfos(schema, tableName);
    const existsToAdd = Math.floor(rng() * 4);
    if (existsToAdd === 0) {
      return query;
    }
    const shuffledRelationships = shuffle(rng, relationships);
    const existsToAddNames = shuffledRelationships.slice(0, existsToAdd);
    existsToAddNames.forEach(rel => {
      const relationshipName = rel.name;
      const options = randomExistsOptions(rng, rel);
      if (rng() < 0.5) {
        const subGenerations: Generation[] = [];
        const origQuery = query;
        query = query.where(({not, exists}) =>
          not(
            exists(
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
              options,
            ),
          ),
        );
        for (const q of subGenerations) {
          generations.push(
            origQuery.where(({not, exists}) =>
              not(exists(relationshipName, _ => q, options)),
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
          options,
        );
        for (const q of subGenerations) {
          generations.push(
            origQuery.whereExists(relationshipName, _ => q, options),
          );
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

export type GeneratedQueryCase = {
  label: string;
  seed: number;
  rng: () => number;
  query: [AnyQuery, AnyQuery[]];
  tags: readonly string[];
};

export type StructuredAstCase = {
  ast: AST;
  label: string;
  tags: readonly string[];
};

type RelationshipInfo = {
  name: string;
  relationship: Relationship;
};

type ExistsCase = {
  suffix: string;
  options: ExistsOptions | undefined;
  tags: readonly string[];
};

const nestedExistsDepths = [2, 4, 6] as const;

export function generateStructuredQueryCases(
  schema: Schema,
  data: Dataset,
  count: number,
  options: {
    seedBase?: number | undefined;
  } = {},
): GeneratedQueryCase[] {
  const seedBase = options.seedBase ?? 0x5eed_0000;
  const astCases = enumerateSmallQueryCases(schema, data).slice(0, count);
  return astCases.map((tc, i) => {
    const seed = seedBase + i;
    const randomizer = generateMersenne53Randomizer(seed);
    const rng = () => randomizer.next();
    const generations = shrinkCandidates(tc.ast).map(ast =>
      astToQuery(schema, ast, 'test'),
    );
    return {
      label: `${tc.label}-${seed}`,
      seed,
      rng,
      query: [astToQuery(schema, tc.ast, 'test'), generations],
      tags: tc.tags,
    };
  });
}

export function enumerateSmallQueryCases(
  schema: Schema,
  data: Dataset,
): StructuredAstCase[] {
  const cases: StructuredAstCase[] = [];
  const tables = Object.keys(schema.tables);

  for (const table of tables) {
    const base: AST = {table};
    cases.push(tag(base, `${table}-base`, ['root']));

    const single = simpleCondition(schema, data, table, 0, '=');
    if (single) {
      cases.push(
        tag({...base, where: single}, `${table}-where-eq`, [
          'where.single',
          'op.=',
        ]),
      );
    }

    const and = compoundWhere(schema, data, table, 'and');
    if (and) {
      cases.push(
        tag({...base, where: and}, `${table}-where-and`, ['where.and']),
      );
    }

    const or = compoundWhere(schema, data, table, 'or');
    if (or) {
      cases.push(tag({...base, where: or}, `${table}-where-or`, ['where.or']));
    }

    const order = ordering(schema, table, 0, 'asc');
    if (order) {
      cases.push(
        tag({...base, orderBy: order, limit: 5}, `${table}-order-limit`, [
          'order.asc',
          'limit.small',
        ]),
      );
    }

    const descOrder = ordering(schema, table, 1, 'desc');
    if (descOrder) {
      cases.push(
        tag(
          {...base, orderBy: descOrder, limit: rowCount(data, table) + 5},
          `${table}-desc-large-limit`,
          ['order.desc', 'limit.large'],
        ),
      );
    }

    for (const targetDepth of nestedExistsDepths) {
      const nestedExists = nestedExistsCondition(
        schema,
        data,
        table,
        targetDepth,
      );
      if (nestedExists) {
        cases.push(
          tag(
            {...base, where: nestedExists.condition},
            `${table}-nested-exists-depth-${nestedExists.depth}`,
            [
              'exists.nested',
              `exists.depth-${nestedExists.depth}`,
              'exists.flip',
            ],
          ),
        );
      }
    }

    for (const rel of relationshipInfos(schema, table).slice(0, 2)) {
      const childTable = destTable(rel.relationship);
      const childWhere = simpleCondition(schema, data, childTable, 0, '=');
      const relatedChild: AST = {
        table: childTable,
        alias: rel.name,
        ...(childWhere ? {where: childWhere} : {}),
        limit: 3,
      };
      cases.push(
        tag(
          {
            ...base,
            related: [relatedSubquery(rel, relatedChild)],
          },
          `${table}-related-${rel.name}`,
          ['related', relationshipTag(rel.relationship)],
        ),
      );

      const existsChild: AST = {
        table: childTable,
        alias: `zsubq_${rel.name}`,
        ...(childWhere ? {where: childWhere} : {}),
      };
      for (const existsCase of existsCases(rel)) {
        cases.push(
          tag(
            {
              ...base,
              where: existsCondition(
                rel,
                existsChild,
                'EXISTS',
                existsCase.options,
              ),
            },
            `${table}-exists-${existsCase.suffix}-${rel.name}`,
            ['exists', ...existsCase.tags, relationshipTag(rel.relationship)],
          ),
        );

        cases.push(
          tag(
            {
              ...base,
              where: existsCondition(
                rel,
                existsChild,
                'NOT EXISTS',
                existsCase.options,
              ),
            },
            `${table}-not-exists-${existsCase.suffix}-${rel.name}`,
            [
              'not-exists',
              ...existsCase.tags,
              relationshipTag(rel.relationship),
            ],
          ),
        );
      }

      if (single) {
        for (const existsCase of existsCases(rel)) {
          cases.push(
            tag(
              {
                ...base,
                where: {
                  type: 'or',
                  conditions: [
                    single,
                    existsCondition(
                      rel,
                      existsChild,
                      'EXISTS',
                      existsCase.options,
                    ),
                  ],
                },
              },
              `${table}-or-exists-${existsCase.suffix}-${rel.name}`,
              [
                'where.or',
                'exists-under-or',
                ...existsCase.tags,
                relationshipTag(rel.relationship),
              ],
            ),
          );
        }
      }
    }
  }

  return cases;
}

function tag(
  ast: AST,
  label: string,
  tags: readonly string[],
): StructuredAstCase {
  return {ast, label, tags};
}

function astToQuery(schema: Schema, ast: AST, system: System): AnyQuery {
  return newQueryImpl(
    schema,
    ast.table,
    ast,
    formatForAst(ast),
    system,
  ) as AnyQuery;
}

function formatForAst(ast: AST): Format {
  const relationships: Record<string, Format> = {};
  for (const related of ast.related ?? []) {
    const name = visibleRelationshipName(related);
    if (name) {
      relationships[name] = formatForAst(visibleAst(related));
    }
  }
  return {
    singular: false,
    relationships,
  };
}

function visibleRelationshipName(
  related: CorrelatedSubquery,
): string | undefined {
  if (!related.hidden) {
    return related.subquery.alias;
  }
  return related.subquery.related?.[0]?.subquery.alias;
}

function visibleAst(related: CorrelatedSubquery): AST {
  if (!related.hidden) {
    return related.subquery;
  }
  return related.subquery.related?.[0]?.subquery ?? related.subquery;
}

function shrinkCandidates(ast: AST): AST[] {
  const ret: AST[] = [{table: ast.table}];
  if (ast.where) {
    ret.push({...ast, where: undefined});
  }
  if (ast.related?.length) {
    ret.push({...ast, related: undefined});
  }
  if (ast.orderBy) {
    ret.push({...ast, orderBy: undefined, start: undefined});
  }
  if (ast.limit !== undefined) {
    ret.push({...ast, limit: undefined});
  }
  if (ast.start) {
    ret.push({...ast, start: undefined});
  }
  ret.push(ast);
  return ret;
}

function relationshipInfos(schema: Schema, table: string): RelationshipInfo[] {
  return Object.entries(schema.relationships[table] ?? {}).map(
    ([name, relationship]) => ({name, relationship}),
  );
}

function destTable(relationship: Relationship): string {
  return relationship.at(-1)!.destSchema;
}

function relationshipTag(relationship: Relationship): string {
  return relationship.length === 1
    ? 'relationship.one-hop'
    : 'relationship.two-hop';
}

function existsCases(rel: RelationshipInfo): readonly ExistsCase[] {
  const cases: ExistsCase[] = [
    {suffix: 'plain', options: undefined, tags: []},
    {
      suffix: 'flip',
      options: {flip: true},
      tags: ['exists.flip'],
    },
  ];
  if (isScalarRelationship(rel)) {
    cases.push(
      {
        suffix: 'scalar',
        options: {scalar: true},
        tags: ['exists.scalar'],
      },
      {
        suffix: 'flip-scalar',
        options: {flip: true, scalar: true},
        tags: ['exists.flip', 'exists.scalar'],
      },
    );
  }
  return cases;
}

function randomExistsOptions(
  rng: Rng,
  rel: RelationshipInfo,
): ExistsOptions | undefined {
  return selectRandom(rng, existsCases(rel)).options;
}

function isScalarRelationship(rel: RelationshipInfo): boolean {
  return rel.relationship.at(-1)?.cardinality === 'one';
}

function existsCondition(
  rel: RelationshipInfo,
  childAst: AST,
  op: 'EXISTS' | 'NOT EXISTS',
  options: ExistsOptions | undefined,
): Condition {
  return {
    type: 'correlatedSubquery',
    related: existsSubquery(rel, childAst, options),
    op,
    ...(options?.flip !== undefined ? {flip: options.flip} : {}),
    ...(isScalarRelationship(rel) && options?.scalar !== undefined
      ? {scalar: options.scalar}
      : {}),
  };
}

function nestedExistsCondition(
  schema: Schema,
  data: Dataset,
  table: string,
  targetDepth: number,
): {condition: Condition; depth: number} | undefined {
  const rel = relationshipInfos(schema, table).find(
    rel => rel.relationship.length === 1,
  );
  if (!rel) {
    return undefined;
  }

  const childTable = destTable(rel.relationship);
  const childWhere = simpleCondition(schema, data, childTable, 0, '=');
  const nested =
    targetDepth === 1
      ? undefined
      : nestedExistsCondition(schema, data, childTable, targetDepth - 1);

  if (targetDepth > 1 && !nested) {
    return undefined;
  }

  const childAst: AST = {
    table: childTable,
    alias: `zsubq_${rel.name}`,
    ...(nested
      ? {where: nested.condition}
      : childWhere
        ? {where: childWhere}
        : {}),
  };
  const options = {
    flip: true,
    ...(nested ? {} : {scalar: true}),
  };
  return {
    condition: existsCondition(rel, childAst, 'EXISTS', options),
    depth: 1 + (nested?.depth ?? 0),
  };
}

function relatedSubquery(
  rel: RelationshipInfo,
  childAst: AST,
): CorrelatedSubquery {
  const relationship = rel.relationship;
  if (relationship.length === 1) {
    const [connection] = relationship;
    return {
      system: 'test',
      correlation: {
        parentField: compoundKey(connection.sourceField),
        childField: compoundKey(connection.destField),
      },
      subquery: {...childAst, alias: rel.name},
    };
  }

  const [first, second] = relationship;
  return {
    system: 'test',
    correlation: {
      parentField: compoundKey(first.sourceField),
      childField: compoundKey(first.destField),
    },
    hidden: true,
    subquery: {
      table: first.destSchema,
      alias: rel.name,
      related: [
        {
          system: 'test',
          correlation: {
            parentField: compoundKey(second.sourceField),
            childField: compoundKey(second.destField),
          },
          subquery: {...childAst, alias: rel.name, limit: undefined},
        },
      ],
    },
  };
}

function existsSubquery(
  rel: RelationshipInfo,
  childAst: AST,
  options?: ExistsOptions | undefined,
): CorrelatedSubquery {
  const relationship = rel.relationship;
  if (relationship.length === 1) {
    const [connection] = relationship;
    return {
      system: 'test',
      correlation: {
        parentField: compoundKey(connection.sourceField),
        childField: compoundKey(connection.destField),
      },
      subquery: {...childAst, alias: `zsubq_${rel.name}`},
    };
  }

  const [first, second] = relationship;
  return {
    system: 'test',
    correlation: {
      parentField: compoundKey(first.sourceField),
      childField: compoundKey(first.destField),
    },
    subquery: {
      table: first.destSchema,
      alias: `zsubq_${rel.name}`,
      where: {
        type: 'correlatedSubquery',
        related: {
          system: 'test',
          correlation: {
            parentField: compoundKey(second.sourceField),
            childField: compoundKey(second.destField),
          },
          subquery: {
            ...childAst,
            alias: `zsubq_zhidden_${rel.name}`,
            limit: undefined,
          },
        },
        op: 'EXISTS',
        ...(options?.flip !== undefined ? {flip: options.flip} : {}),
        ...(options?.scalar !== undefined ? {scalar: options.scalar} : {}),
      },
    },
  };
}

function compoundKey(fields: readonly string[]): CompoundKey {
  if (fields.length === 0) {
    throw new Error('Expected non-empty compound key');
  }
  return fields as CompoundKey;
}

function compoundWhere(
  schema: Schema,
  data: Dataset,
  table: string,
  type: 'and' | 'or',
): Condition | undefined {
  const first = simpleCondition(schema, data, table, 0, '=');
  const second = simpleCondition(
    schema,
    data,
    table,
    1,
    comparisonOp(schema, table, 1),
  );
  if (!first || !second) {
    return undefined;
  }
  return {
    type,
    conditions: [first, second],
  };
}

function simpleCondition(
  schema: Schema,
  data: Dataset,
  table: string,
  columnOffset: number,
  preferredOp: SimpleOperator,
): Condition | undefined {
  const columns = comparableColumns(schema, table);
  const column = columns[columnOffset % columns.length];
  if (!column) {
    return undefined;
  }
  const value = literalFor(schema, data, table, column, preferredOp);
  if (value === undefined) {
    return undefined;
  }
  return {
    type: 'simple',
    op: opForValue(preferredOp, value),
    left: {type: 'column', name: column},
    right: {type: 'literal', value},
  };
}

function comparableColumns(schema: Schema, table: string): string[] {
  return Object.entries(schema.tables[table].columns)
    .filter(([, column]) => column.type !== 'json')
    .map(([name]) => name);
}

function ordering(
  schema: Schema,
  table: string,
  columnOffset: number,
  direction: 'asc' | 'desc',
): Ordering | undefined {
  const columns = comparableColumns(schema, table);
  const column = columns[columnOffset % columns.length];
  return column ? [[column, direction]] : undefined;
}

function comparisonOp(
  schema: Schema,
  table: string,
  columnOffset: number,
): SimpleOperator {
  const columns = comparableColumns(schema, table);
  const column = columns[columnOffset % columns.length];
  const type = schema.tables[table].columns[column]?.type;
  return type === 'string' ? 'ILIKE' : '>=';
}

function literalFor(
  schema: Schema,
  data: Dataset,
  table: string,
  column: string,
  op: SimpleOperator,
): LiteralValue | undefined {
  const rows = data[table] ?? [];
  const present = rows.find(row => row[column] !== undefined)?.[column];
  const columnSchema = schema.tables[table].columns[column];
  if (
    op === 'LIKE' ||
    op === 'ILIKE' ||
    op === 'NOT LIKE' ||
    op === 'NOT ILIKE'
  ) {
    if (typeof present !== 'string') {
      return undefined;
    }
    return `%${present.slice(0, Math.min(3, present.length))}%`;
  }
  if (op === 'IN' || op === 'NOT IN') {
    const values = rows
      .map(row => row[column])
      .filter(
        (value): value is string | number | boolean =>
          typeof value === 'string' ||
          typeof value === 'number' ||
          typeof value === 'boolean',
      )
      .slice(0, 3);
    return values.length ? values : undefined;
  }
  if (present === null && op !== 'IS' && op !== 'IS NOT') {
    return undefined;
  }
  if (present !== undefined) {
    return present as LiteralValue;
  }
  if (columnSchema.optional) {
    return null;
  }
  return undefined;
}

function opForValue(op: SimpleOperator, value: LiteralValue): SimpleOperator {
  if (value === null) {
    return op === 'IS NOT' ? 'IS NOT' : 'IS';
  }
  return op;
}

function rowCount(data: Dataset, table: string): number {
  return data[table]?.length ?? 0;
}
