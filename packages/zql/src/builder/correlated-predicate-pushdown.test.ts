import {describe, expect, test} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import {
  planIdSymbol,
  type AST,
  type Condition,
  type Conjunction,
  type CorrelatedSubquery,
  type CorrelatedSubqueryCondition,
  type LiteralValue,
  type SimpleCondition,
  type SimpleOperator,
} from '../../../zero-protocol/src/ast.ts';
import type {SchemaValue} from '../../../zero-types/src/schema-value.ts';
import {
  MAX_PUSHED_IN_VALUES,
  pushDownCorrelatedPredicates,
} from './correlated-predicate-pushdown.ts';

const columns: Record<string, Record<string, SchemaValue>> = {
  user: {
    userID: {type: 'string'},
    orgID: {type: 'number'},
    name: {type: 'string'},
    role: {type: 'string'},
    prefs: {type: 'json'},
  },
  reading: {
    id: {type: 'string'},
    userID: {type: 'string'},
    orgID: {type: 'number'},
    workID: {type: 'string'},
    userNum: {type: 'number'},
    prefs: {type: 'json'},
  },
  works: {
    id: {type: 'string'},
    title: {type: 'string'},
  },
  covers: {
    id: {type: 'string'},
    workID: {type: 'string'},
  },
};

function columnsOf(table: string) {
  return must(columns[table], `unknown table ${table}`);
}

function push(ast: AST): AST {
  return pushDownCorrelatedPredicates(ast, columnsOf);
}

function cmp(
  column: string,
  op: SimpleOperator,
  value: LiteralValue,
): SimpleCondition {
  return {
    type: 'simple',
    left: {type: 'column', name: column},
    op,
    right: {type: 'literal', value},
  };
}

function and(...conditions: Condition[]): Condition {
  return {type: 'and', conditions};
}

function or(...conditions: Condition[]): Condition {
  return {type: 'or', conditions};
}

function related(
  parentField: [string, ...string[]],
  childField: [string, ...string[]],
  subquery: AST,
): CorrelatedSubquery {
  return {correlation: {parentField, childField}, subquery};
}

function exists(
  parentField: [string, ...string[]],
  childField: [string, ...string[]],
  subquery: AST,
  op: 'EXISTS' | 'NOT EXISTS' = 'EXISTS',
): CorrelatedSubqueryCondition {
  return {
    type: 'correlatedSubquery',
    op,
    related: related(parentField, childField, subquery),
  };
}

function reading(where?: Condition): AST {
  return {table: 'reading', alias: 'reading', where};
}

/** `user` with `where` and a `reading` relationship on `userID`. */
function userReading(where: Condition, readingWhere?: Condition): AST {
  return {
    table: 'user',
    where,
    related: [related(['userID'], ['userID'], reading(readingWhere))],
  };
}

/** The `where` that `push` gives the `reading` relationship. */
function pushedReadingWhere(ast: AST): Condition | undefined {
  return must(push(ast).related)[0].subquery.where;
}

describe('related', () => {
  test.each([
    ['=', 'u1'],
    ['IS', 'u1'],
    ['IS', null],
    ['IN', ['u1', 'u2']],
  ] as const)('pushes %s', (op, value) => {
    expect(pushedReadingWhere(userReading(cmp('userID', op, value)))).toEqual(
      cmp('userID', op, value),
    );
  });

  test('pushes IN with at most MAX_PUSHED_IN_VALUES values', () => {
    const atCap = Array.from({length: MAX_PUSHED_IN_VALUES}, (_, i) => `u${i}`);
    expect(pushedReadingWhere(userReading(cmp('userID', 'IN', atCap)))).toEqual(
      cmp('userID', 'IN', atCap),
    );

    const overCap = [...atCap, 'one-more'];
    expect(
      pushedReadingWhere(userReading(cmp('userID', 'IN', overCap))),
    ).toBeUndefined();
  });

  test.each([
    ['!=', 'u1'],
    ['IS NOT', null],
    ['NOT IN', ['u1']],
    ['<', 'u1'],
    ['>=', 'u1'],
    ['LIKE', 'u%'],
    ['ILIKE', 'u%'],
  ] as const)('does not push %s', (op, value) => {
    expect(
      pushedReadingWhere(userReading(cmp('userID', op, value))),
    ).toBeUndefined();
  });

  test('pushes the conjuncts on the correlation column only', () => {
    expect(
      pushedReadingWhere(
        userReading(
          and(cmp('name', '=', 'bob'), cmp('userID', '=', 'u1')),
          cmp('workID', '=', 'w1'),
        ),
      ),
    ).toEqual(and(cmp('workID', '=', 'w1'), cmp('userID', '=', 'u1')));
  });

  test('pushes conjuncts of nested ANDs', () => {
    expect(
      pushedReadingWhere(
        userReading(
          and(cmp('name', '=', 'bob'), and(cmp('userID', '=', 'u1'))),
        ),
      ),
    ).toEqual(cmp('userID', '=', 'u1'));
  });

  test('does not push conditions under an OR', () => {
    expect(
      pushedReadingWhere(
        userReading(or(cmp('userID', '=', 'u1'), cmp('role', '=', 'admin'))),
      ),
    ).toBeUndefined();
    expect(
      pushedReadingWhere(
        userReading(
          and(
            cmp('name', '=', 'bob'),
            or(cmp('userID', '=', 'u1'), cmp('userID', '=', 'u2')),
          ),
        ),
      ),
    ).toBeUndefined();
  });

  test('flattens the child where', () => {
    expect(
      pushedReadingWhere(
        userReading(
          cmp('userID', '=', 'u1'),
          and(cmp('workID', '=', 'w1'), cmp('orgID', '=', 1)),
        ),
      ),
    ).toEqual(
      and(
        cmp('workID', '=', 'w1'),
        cmp('orgID', '=', 1),
        cmp('userID', '=', 'u1'),
      ),
    );
  });

  test('renames to the child column', () => {
    const ast: AST = {
      table: 'reading',
      where: cmp('workID', '=', 'w1'),
      related: [related(['workID'], ['id'], {table: 'works', alias: 'works'})],
    };
    expect(must(push(ast).related)[0].subquery.where).toEqual(
      cmp('id', '=', 'w1'),
    );
  });

  test('compound correlations push only the columns that have facts', () => {
    const ast: AST = {
      table: 'user',
      where: and(cmp('orgID', '=', 1), cmp('name', '=', 'bob')),
      related: [related(['userID', 'orgID'], ['userID', 'orgID'], reading())],
    };
    expect(must(push(ast).related)[0].subquery.where).toEqual(
      cmp('orgID', '=', 1),
    );

    const both: AST = {
      ...ast,
      where: and(cmp('orgID', '=', 1), cmp('userID', '=', 'u1')),
    };
    expect(must(push(both).related)[0].subquery.where).toEqual(
      and(cmp('orgID', '=', 1), cmp('userID', '=', 'u1')),
    );
  });

  test('a parent column correlated to two child columns pushes to both', () => {
    const ast: AST = {
      table: 'user',
      where: cmp('userID', '=', 'u1'),
      related: [related(['userID', 'userID'], ['userID', 'workID'], reading())],
    };
    expect(must(push(ast).related)[0].subquery.where).toEqual(
      and(cmp('userID', '=', 'u1'), cmp('workID', '=', 'u1')),
    );
  });

  test('skips columns with different Zero types', () => {
    const ast: AST = {
      table: 'user',
      where: cmp('userID', '=', 'u1'),
      related: [related(['userID'], ['userNum'], reading())],
    };
    expect(must(push(ast).related)[0].subquery.where).toBeUndefined();
  });

  test('skips json columns', () => {
    const ast: AST = {
      table: 'user',
      where: cmp('prefs', '=', 'x'),
      related: [related(['prefs'], ['prefs'], reading())],
    };
    expect(must(push(ast).related)[0].subquery.where).toBeUndefined();
  });

  test('skips static parameters', () => {
    const where: SimpleCondition = {
      type: 'simple',
      left: {type: 'column', name: 'userID'},
      op: '=',
      right: {type: 'static', anchor: 'authData', field: 'sub'},
    };
    expect(pushedReadingWhere(userReading(where))).toBeUndefined();
  });

  test('skips conditions with a literal on the left', () => {
    const where: SimpleCondition = {
      type: 'simple',
      left: {type: 'literal', value: 'u1'},
      op: '=',
      right: {type: 'literal', value: 'u1'},
    };
    expect(pushedReadingWhere(userReading(where))).toBeUndefined();
  });

  test('pushes along a chain that correlates on the same column', () => {
    // user -> reading -> works -> covers. Only `reading` is correlated on
    // `userID`, so the chain stops at `works`.
    const ast: AST = {
      table: 'user',
      where: cmp('userID', '=', 'u1'),
      related: [
        related(['userID'], ['userID'], {
          ...reading(),
          related: [
            related(['workID'], ['id'], {
              table: 'works',
              alias: 'works',
              related: [
                related(['id'], ['workID'], {table: 'covers', alias: 'covers'}),
              ],
            }),
          ],
        }),
      ],
    };
    expect(push(ast)).toEqual({
      table: 'user',
      where: cmp('userID', '=', 'u1'),
      related: [
        related(['userID'], ['userID'], {
          ...reading(cmp('userID', '=', 'u1')),
          related: [
            related(['workID'], ['id'], {
              table: 'works',
              alias: 'works',
              related: [
                related(['id'], ['workID'], {table: 'covers', alias: 'covers'}),
              ],
            }),
          ],
        }),
      ],
    });
  });

  test('pushes transitively', () => {
    const ast: AST = {
      table: 'user',
      where: cmp('userID', '=', 'u1'),
      related: [
        related(['userID'], ['userID'], {
          ...reading(),
          related: [
            related(['userID'], ['userID'], {
              table: 'user',
              alias: 'readers',
            }),
          ],
        }),
      ],
    };
    const result = push(ast);
    const readingAST = must(result.related)[0].subquery;
    expect(readingAST.where).toEqual(cmp('userID', '=', 'u1'));
    expect(must(readingAST.related)[0].subquery.where).toEqual(
      cmp('userID', '=', 'u1'),
    );
  });
});

describe('EXISTS', () => {
  test.each(['EXISTS', 'NOT EXISTS'] as const)(
    'pushes the conjuncts that enclose %s',
    op => {
      const ast: AST = {
        table: 'user',
        where: and(
          cmp('userID', '=', 'u1'),
          exists(['userID'], ['userID'], reading(), op),
        ),
      };
      expect(push(ast).where).toEqual(
        and(
          cmp('userID', '=', 'u1'),
          exists(['userID'], ['userID'], reading(cmp('userID', '=', 'u1')), op),
        ),
      );
    },
  );

  test('pushes the conjuncts of every enclosing AND', () => {
    const ast: AST = {
      table: 'user',
      where: and(
        cmp('userID', '=', 'u1'),
        and(
          cmp('orgID', '=', 1),
          exists(['userID', 'orgID'], ['userID', 'orgID'], reading()),
        ),
      ),
    };
    expect(push(ast).where).toEqual(
      and(
        cmp('userID', '=', 'u1'),
        and(
          cmp('orgID', '=', 1),
          exists(
            ['userID', 'orgID'],
            ['userID', 'orgID'],
            reading(and(cmp('userID', '=', 'u1'), cmp('orgID', '=', 1))),
          ),
        ),
      ),
    );
  });

  test('pushes the conjuncts of an AND nested in an enclosing AND', () => {
    const ast: AST = {
      table: 'user',
      where: and(
        exists(['userID'], ['userID'], reading()),
        and(cmp('name', '=', 'bob'), cmp('userID', '=', 'u1')),
      ),
    };
    expect((push(ast).where as Conjunction).conditions[0]).toEqual(
      exists(['userID'], ['userID'], reading(cmp('userID', '=', 'u1'))),
    );
  });

  test('facts from above an OR are pushed into EXISTS below it', () => {
    const ast: AST = {
      table: 'user',
      where: and(
        cmp('userID', '=', 'u1'),
        or(
          cmp('role', '=', 'admin'),
          exists(['userID'], ['userID'], reading()),
        ),
      ),
    };
    expect(push(ast).where).toEqual(
      and(
        cmp('userID', '=', 'u1'),
        or(
          cmp('role', '=', 'admin'),
          exists(['userID'], ['userID'], reading(cmp('userID', '=', 'u1'))),
        ),
      ),
    );
  });

  test('conditions under an OR are not facts for its EXISTS', () => {
    const ast: AST = {
      table: 'user',
      where: or(
        and(cmp('userID', '=', 'u1'), cmp('role', '=', 'admin')),
        exists(['userID'], ['userID'], reading()),
      ),
    };
    expect(push(ast)).toBe(ast);
  });

  test('conditions in an AND under an OR are facts for EXISTS in that AND', () => {
    const ast: AST = {
      table: 'user',
      where: or(
        cmp('role', '=', 'admin'),
        and(
          cmp('userID', '=', 'u1'),
          exists(['userID'], ['userID'], reading()),
        ),
      ),
    };
    expect(push(ast).where).toEqual(
      or(
        cmp('role', '=', 'admin'),
        and(
          cmp('userID', '=', 'u1'),
          exists(['userID'], ['userID'], reading(cmp('userID', '=', 'u1'))),
        ),
      ),
    );
  });

  test('a lone EXISTS gets nothing', () => {
    const ast: AST = {
      table: 'user',
      where: exists(['userID'], ['userID'], reading()),
    };
    expect(push(ast)).toBe(ast);
  });

  test('pushes into EXISTS inside the child after pushing into the child', () => {
    // The `reading` relationship gets `userID = u1`, which is then a fact for
    // the EXISTS in `reading.where`.
    const ast = userReading(
      cmp('userID', '=', 'u1'),
      exists(['userID'], ['userID'], {table: 'user', alias: 'owner'}),
    );
    expect(pushedReadingWhere(ast)).toEqual(
      and(
        exists(['userID'], ['userID'], {
          table: 'user',
          alias: 'owner',
          where: cmp('userID', '=', 'u1'),
        }),
        cmp('userID', '=', 'u1'),
      ),
    );
  });

  test('keeps flip, scalar and the plan id', () => {
    const csq: CorrelatedSubqueryCondition = {
      ...exists(['userID'], ['userID'], reading()),
      flip: true,
      scalar: true,
      [planIdSymbol]: 7,
    };
    const ast: AST = {
      table: 'user',
      where: and(cmp('userID', '=', 'u1'), csq),
    };
    const pushed = (push(ast).where as Conjunction)
      .conditions[1] as CorrelatedSubqueryCondition;
    expect(pushed.flip).toBe(true);
    expect(pushed.scalar).toBe(true);
    expect(pushed[planIdSymbol]).toBe(7);
    expect(pushed.related.subquery.where).toEqual(cmp('userID', '=', 'u1'));
  });
});

test('does not change its input', () => {
  const ast: AST = {
    table: 'user',
    where: and(
      cmp('userID', '=', 'u1'),
      exists(['userID'], ['userID'], reading(cmp('workID', '=', 'w1'))),
    ),
    related: [
      related(['userID'], ['userID'], {
        ...reading(),
        related: [related(['workID'], ['id'], {table: 'works', alias: 'w'})],
      }),
    ],
  };
  const copy = structuredClone(ast);
  push(ast);
  expect(ast).toEqual(copy);
});

test('returns the same objects when nothing is pushed', () => {
  const works: CorrelatedSubquery = related(['workID'], ['id'], {
    table: 'works',
    alias: 'works',
  });
  const ast: AST = {
    table: 'user',
    where: and(
      cmp('userID', '=', 'u1'),
      exists(['orgID'], ['orgID'], reading()),
    ),
    related: [
      related(['userID'], ['userID'], {...reading(), related: [works]}),
    ],
  };
  const result = push(ast);
  expect(result).not.toBe(ast);
  expect(result.where).toBe(ast.where);
  const readingAST = must(result.related)[0].subquery;
  expect(readingAST.where).toEqual(cmp('userID', '=', 'u1'));
  expect(must(readingAST.related)[0]).toBe(works);

  const nothing: AST = {
    table: 'user',
    where: cmp('name', '=', 'bob'),
    related: [related(['userID'], ['userID'], reading())],
  };
  expect(push(nothing)).toBe(nothing);
});
