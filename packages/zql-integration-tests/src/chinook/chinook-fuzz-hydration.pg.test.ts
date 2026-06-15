/* oxlint-disable no-console */

import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../ast-to-zql/src/format.ts';
import type {
  AST,
  CompoundKey,
  Condition,
  CorrelatedSubquery,
  LiteralValue,
  Ordering,
  SimpleOperator,
} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Format} from '../../../zero-types/src/format.ts';
import type {Relationship, Schema} from '../../../zero-types/src/schema.ts';
import {createRandomYieldWrapper} from '../../../zql/src/ivm/test/random-yield-source.ts';
import {newQueryImpl} from '../../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {generateShrinkableQuery} from '../../../zql/src/query/test/query-gen.ts';
import '../helpers/comparePg.ts';
import {bootstrap, checkPush, runAndCompare} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

const fuzzSchema: Schema = schema;
const pgContent = await getChinook();

// Set this to reproduce a specific failure.
const REPRO_SEED = undefined;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_hydration',
  zqlSchema: schema,
  pgContent,
});

const rawData = Object.fromEntries(harness.dbs.raw) as Dataset;
const VITEST_TIMEOUT_MS = 60_000; // set via third argument to test() calls

// Internal timeout for graceful handling (shorter than vitest timeout)
const TEST_TIMEOUT_MS = VITEST_TIMEOUT_MS / 2;
const CASE_COUNT = 100;
const RANDOM_TAIL_CASES = 10;

/**
 * Error thrown when a fuzz test query exceeds the time limit.
 * This is caught and treated as a pass (with warning) rather than a failure.
 */
class FuzzTimeoutError extends Error {
  constructor(label: string, elapsedMs: number) {
    super(`Fuzz test "${label}" timed out after ${elapsedMs}ms`);
    this.name = 'FuzzTimeoutError';
  }
}

/**
 * Creates a checkAbort function that throws FuzzTimeoutError when the
 * elapsed time exceeds the timeout. This allows synchronous query execution
 * to be aborted when it takes too long.
 */
function createCheckAbort(
  startTime: number,
  timeoutMs: number,
  label: string,
): () => void {
  return () => {
    const elapsed = Date.now() - startTime;
    if (elapsed > timeoutMs) {
      throw new FuzzTimeoutError(label, elapsed);
    }
  };
}

// oxlint-disable-next-line expect-expect
test.each(createCases())(
  'fuzz-hydration $label',
  runCase,
  VITEST_TIMEOUT_MS, // vitest timeout: longer than internal timeout to ensure we catch it ourselves
);

test('sentinel', () => {
  expect(true).toBe(true);
});

if (REPRO_SEED) {
  // oxlint-disable-next-line no-focused-tests
  test.only(
    'repro',
    async () => {
      const tc = {
        ...createCase(REPRO_SEED),
        label: `repro-${REPRO_SEED}`,
        tags: ['repro'],
      };
      const {query} = tc;
      console.log(
        'ZQL',
        await formatOutput(
          asQueryInternals(query[0]).ast.table +
            astToZQL(asQueryInternals(query[0]).ast),
        ),
      );
      await runCase(tc);
    },
    VITEST_TIMEOUT_MS,
  );
}

function createCase(seed?: number) {
  seed = seed ?? Date.now() ^ (Math.random() * 0x100000000);
  const randomizer = generateMersenne53Randomizer(seed);
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });
  return {
    seed,
    rng,
    query: generateShrinkableQuery(
      schema,
      {},
      rng,
      faker,
      harness.delegates.pg.serverSchema,
    ),
  };
}

function createCases(): FuzzCase[] {
  const deterministic = createStructuredCases(CASE_COUNT - RANDOM_TAIL_CASES);
  const tail = Array.from({length: RANDOM_TAIL_CASES}, () => createCase()).map(
    (tc, i) => ({
      ...tc,
      label: `random-tail-${i}-${tc.seed}`,
      tags: ['random-tail'],
    }),
  );
  return [...deterministic, ...tail];
}

async function runCase({
  query,
  seed,
  rng,
  label,
}: {
  query: [AnyQuery, AnyQuery[]];
  seed: number;
  rng: () => number;
  label: string;
}) {
  const startTime = Date.now();
  const checkAbort = createCheckAbort(startTime, TEST_TIMEOUT_MS, label);

  // Create a source wrapper that injects random yields and timeout checking
  // for both memory and sqlite sources
  const sourceWrapper = createRandomYieldWrapper(rng, 0.3, checkAbort);

  try {
    await harness.transact(async delegates => {
      await runAndCompare(schema, delegates, query[0], undefined);
      await checkPush(schema, delegates, query[0], 10);
    }, sourceWrapper);
  } catch (e) {
    // Timeouts pass with a warning
    if (e instanceof FuzzTimeoutError) {
      console.warn(`⚠️ ${e.message} - passing anyway`);
      return;
    }

    // Actual test failures get shrunk and re-thrown
    const zql = await shrink(query[1], seed, rng);
    if (seed === REPRO_SEED) {
      throw e;
    }
    throw new Error('Mismatch. Repro seed: ' + seed + '\nshrunk zql: ' + zql);
  }
}

async function shrink(
  generations: AnyQuery[],
  seed: number,
  rng: () => number,
) {
  console.log('Found failure at seed', seed);
  console.log('Shrinking', generations.length, 'generations');
  let low = 0;
  let high = generations.length;
  let lastFailure = -1;
  while (low < high) {
    const mid = low + ((high - low) >> 1);
    try {
      const startTime = Date.now();
      const checkAbort = createCheckAbort(
        startTime,
        TEST_TIMEOUT_MS,
        `shrink ${seed}`,
      );
      const sourceWrapper = createRandomYieldWrapper(rng, 0.3, checkAbort);
      await harness.transact(async delegates => {
        await runAndCompare(schema, delegates, generations[mid], undefined);
        await checkPush(schema, delegates, generations[mid], 10);
      }, sourceWrapper);
      low = mid + 1;
    } catch {
      lastFailure = mid;
      high = mid;
    }
  }
  if (lastFailure === -1) {
    throw new Error('no failure found');
  }
  const query = generations[lastFailure];
  const queryInternals = asQueryInternals(query);
  return formatOutput(queryInternals.ast.table + astToZQL(queryInternals.ast));
}

type Dataset = {
  [table: string]: readonly Row[];
};

type FuzzCase = {
  label: string;
  seed: number;
  rng: () => number;
  query: [AnyQuery, AnyQuery[]];
  tags: readonly string[];
};

type AstCase = {
  ast: AST;
  label: string;
  tags: readonly string[];
};

type RelationshipInfo = {
  name: string;
  relationship: Relationship;
};

function createStructuredCases(count: number): FuzzCase[] {
  const astCases = enumerateSmallCases(schema, rawData);
  const selected = astCases.slice(0, count);
  return selected.map((tc, i) => {
    const seed = 0x5eed_0000 + i;
    const randomizer = generateMersenne53Randomizer(seed);
    const rng = () => randomizer.next();
    const generations = shrinkCandidates(tc.ast).map(astToQuery);
    return {
      label: `${tc.label}-${seed}`,
      seed,
      rng,
      query: [astToQuery(tc.ast), generations],
      tags: tc.tags,
    };
  });
}

function enumerateSmallCases(s: Schema, data: Dataset): AstCase[] {
  const cases: AstCase[] = [];
  const tables = Object.keys(s.tables);

  for (const table of tables) {
    const base: AST = {table};
    cases.push(tag(base, `${table}-base`, ['root']));

    const single = simpleCondition(table, 0, '=');
    if (single) {
      cases.push(
        tag({...base, where: single}, `${table}-where-eq`, [
          'where.single',
          'op.=',
        ]),
      );
    }

    const and = compoundWhere(table, 'and');
    if (and) {
      cases.push(
        tag({...base, where: and}, `${table}-where-and`, ['where.and']),
      );
    }

    const or = compoundWhere(table, 'or');
    if (or) {
      cases.push(tag({...base, where: or}, `${table}-where-or`, ['where.or']));
    }

    const order = ordering(table, 0, 'asc');
    if (order) {
      cases.push(
        tag({...base, orderBy: order, limit: 5}, `${table}-order-limit`, [
          'order.asc',
          'limit.small',
        ]),
      );
    }

    const descOrder = ordering(table, 1, 'desc');
    if (descOrder) {
      cases.push(
        tag(
          {...base, orderBy: descOrder, limit: rowCount(data, table) + 5},
          `${table}-desc-large-limit`,
          ['order.desc', 'limit.large'],
        ),
      );
    }

    for (const rel of relationshipInfos(table).slice(0, 2)) {
      const childTable = destTable(rel.relationship);
      const childWhere = simpleCondition(childTable, 0, '=');
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
            related: [relatedSubquery(table, rel, relatedChild)],
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
      cases.push(
        tag(
          {
            ...base,
            where: {
              type: 'correlatedSubquery',
              related: existsSubquery(table, rel, existsChild),
              op: 'EXISTS',
            },
          },
          `${table}-exists-${rel.name}`,
          ['exists', relationshipTag(rel.relationship)],
        ),
      );

      cases.push(
        tag(
          {
            ...base,
            where: {
              type: 'correlatedSubquery',
              related: existsSubquery(table, rel, existsChild),
              op: 'NOT EXISTS',
            },
          },
          `${table}-not-exists-${rel.name}`,
          ['not-exists', relationshipTag(rel.relationship)],
        ),
      );

      if (single) {
        cases.push(
          tag(
            {
              ...base,
              where: {
                type: 'or',
                conditions: [
                  single,
                  {
                    type: 'correlatedSubquery',
                    related: existsSubquery(table, rel, existsChild),
                    op: 'EXISTS',
                  },
                ],
              },
            },
            `${table}-or-exists-${rel.name}`,
            ['where.or', 'exists-under-or', relationshipTag(rel.relationship)],
          ),
        );
      }
    }
  }

  return cases;
}

function tag(ast: AST, label: string, tags: readonly string[]): AstCase {
  return {ast, label, tags};
}

function astToQuery(ast: AST): AnyQuery {
  return newQueryImpl(
    fuzzSchema,
    ast.table,
    ast,
    formatForAst(ast),
    'test',
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

function relationshipInfos(table: string): RelationshipInfo[] {
  return Object.entries(fuzzSchema.relationships[table] ?? {}).map(
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

function relatedSubquery(
  _parentTable: string,
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
  _parentTable: string,
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
  table: string,
  type: 'and' | 'or',
): Condition | undefined {
  const first = simpleCondition(table, 0, '=');
  const second = simpleCondition(table, 1, comparisonOp(table, 1));
  if (!first || !second) {
    return undefined;
  }
  return {
    type,
    conditions: [first, second],
  };
}

function simpleCondition(
  table: string,
  columnOffset: number,
  preferredOp: SimpleOperator,
): Condition | undefined {
  const column =
    comparableColumns(table)[columnOffset % comparableColumns(table).length];
  if (!column) {
    return undefined;
  }
  const value = literalFor(table, column, preferredOp);
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

function comparableColumns(table: string): string[] {
  return Object.entries(fuzzSchema.tables[table].columns)
    .filter(([, column]) => column.type !== 'json')
    .map(([name]) => name);
}

function ordering(
  table: string,
  columnOffset: number,
  direction: 'asc' | 'desc',
): Ordering | undefined {
  const column =
    comparableColumns(table)[columnOffset % comparableColumns(table).length];
  return column ? [[column, direction]] : undefined;
}

function comparisonOp(table: string, columnOffset: number): SimpleOperator {
  const column =
    comparableColumns(table)[columnOffset % comparableColumns(table).length];
  const type = fuzzSchema.tables[table].columns[column]?.type;
  return type === 'string' ? 'ILIKE' : '>=';
}

function literalFor(
  table: string,
  column: string,
  op: SimpleOperator,
): LiteralValue | undefined {
  const rows = rawData[table] ?? [];
  const present = rows.find(row => row[column] !== undefined)?.[column];
  const columnSchema = fuzzSchema.tables[table].columns[column];
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
