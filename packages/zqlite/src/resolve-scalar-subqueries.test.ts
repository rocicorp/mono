import {expect, test} from 'vitest';
import type {
  AST,
  Condition,
  Conjunction,
  CorrelatedSubqueryCondition,
  Disjunction,
  SimpleCondition,
} from '../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
import type {
  SchemaValue,
  ValueType,
} from '../../zero-schema/src/table-schema.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {schema} from '../../zql/src/query/test/test-schemas.ts';
import {
  extractLiteralEqualityConstraints,
  isSimpleSubquery,
  resolveSimpleScalarSubqueries,
} from './resolve-scalar-subqueries.ts';

function ast(q: AnyQuery): AST {
  return asQueryInternals(q).ast;
}

type TestSpec = {
  tableSpec: {uniqueKeys: PrimaryKey[]};
  zqlSpec: Record<string, SchemaValue>;
};

function makeTableSpecs(
  entries: Record<string, PrimaryKey[]>,
  columns: Record<string, Record<string, ValueType>> = {},
): Map<string, TestSpec> {
  const map = new Map<string, TestSpec>();
  for (const [table, uniqueKeys] of Object.entries(entries)) {
    map.set(table, {
      tableSpec: {uniqueKeys},
      zqlSpec: Object.fromEntries(
        Object.entries(columns[table] ?? {}).map(([col, type]) => [
          col,
          {type},
        ]),
      ),
    });
  }
  return map;
}

const ALWAYS_FALSE: SimpleCondition = {
  type: 'simple',
  op: '=',
  left: {type: 'literal', value: 1},
  right: {type: 'literal', value: 0},
};

const ALWAYS_TRUE: SimpleCondition = {
  type: 'simple',
  op: '=',
  left: {type: 'literal', value: 1},
  right: {type: 'literal', value: 1},
};

// ---------- extractLiteralEqualityConstraints ----------

test('extractLiteralEqualityConstraints: simple column = literal', () => {
  const cond: Condition = {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'id'},
    right: {type: 'literal', value: '42'},
  };
  const constraints = extractLiteralEqualityConstraints(cond);
  expect(constraints).toEqual(new Map([['id', '42']]));
});

test('extractLiteralEqualityConstraints: ignores non-equality operators', () => {
  const cond: Condition = {
    type: 'simple',
    op: '>',
    left: {type: 'column', name: 'id'},
    right: {type: 'literal', value: 10},
  };
  const constraints = extractLiteralEqualityConstraints(cond);
  expect(constraints.size).toBe(0);
});

test('extractLiteralEqualityConstraints: collects from AND', () => {
  const cond: Condition = {
    type: 'and',
    conditions: [
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 1},
      },
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'b'},
        right: {type: 'literal', value: 2},
      },
    ],
  };
  const constraints = extractLiteralEqualityConstraints(cond);
  expect(constraints).toEqual(
    new Map([
      ['a', 1],
      ['b', 2],
    ]),
  );
});

test('extractLiteralEqualityConstraints: does not descend into OR', () => {
  const cond: Condition = {
    type: 'or',
    conditions: [
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 1},
      },
    ],
  };
  const constraints = extractLiteralEqualityConstraints(cond);
  expect(constraints.size).toBe(0);
});

test('extractLiteralEqualityConstraints: ignores column = column', () => {
  // Column references are excluded from the `right` position in the type,
  // but at runtime they could appear via JSON. Cast to test defensive behavior.
  const cond = {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'a'},
    right: {type: 'column', name: 'b'},
  } as unknown as Condition;
  const constraints = extractLiteralEqualityConstraints(cond);
  expect(constraints.size).toBe(0);
});

// ---------- isSimpleSubquery ----------

test('isSimpleSubquery: true when unique key fully constrained', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const subquery: AST = {
    table: 'users',
    where: {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'id'},
      right: {type: 'literal', value: '0001'},
    },
  };
  expect(isSimpleSubquery(subquery, specs)).toBe(true);
});

test('isSimpleSubquery: true with composite unique key', () => {
  const specs = makeTableSpecs({issueLabel: [['issueId', 'labelId']]});
  const subquery: AST = {
    table: 'issueLabel',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'issueId'},
          right: {type: 'literal', value: '1'},
        },
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'labelId'},
          right: {type: 'literal', value: '2'},
        },
      ],
    },
  };
  expect(isSimpleSubquery(subquery, specs)).toBe(true);
});

test('isSimpleSubquery: false when unique key partially constrained', () => {
  const specs = makeTableSpecs({issueLabel: [['issueId', 'labelId']]});
  const subquery: AST = {
    table: 'issueLabel',
    where: {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'issueId'},
      right: {type: 'literal', value: '1'},
    },
  };
  expect(isSimpleSubquery(subquery, specs)).toBe(false);
});

test('isSimpleSubquery: false when no where clause', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const subquery: AST = {table: 'users'};
  expect(isSimpleSubquery(subquery, specs)).toBe(false);
});

test('isSimpleSubquery: false when table not in specs', () => {
  const specs = makeTableSpecs({});
  const subquery: AST = {
    table: 'unknown',
    where: {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'id'},
      right: {type: 'literal', value: '1'},
    },
  };
  expect(isSimpleSubquery(subquery, specs)).toBe(false);
});

test('isSimpleSubquery: true if any unique key is satisfied', () => {
  const specs = makeTableSpecs({
    users: [['id'], ['email', 'tenant']],
  });
  const subquery: AST = {
    table: 'users',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'email'},
          right: {type: 'literal', value: 'a@b.com'},
        },
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'tenant'},
          right: {type: 'literal', value: 't1'},
        },
      ],
    },
  };
  expect(isSimpleSubquery(subquery, specs)).toBe(true);
});

// ---------- resolveSimpleScalarSubqueries ----------

test('resolves a simple scalar subquery to a literal condition', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['ownerId'],
          childField: ['name'],
        },
        subquery: {
          table: 'users',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: '0001'},
          },
        },
      },
    },
  };

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    (_subAST, _field) => 'Alice',
  );

  expect(resolved.where).toEqual({
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'ownerId'},
    right: {type: 'literal', value: 'Alice'},
  });
  expect(companions).toHaveLength(1);
  expect(companions[0].ast.table).toBe('users');
});

test('NOT EXISTS scalar resolves with IS NOT operator', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'correlatedSubquery',
      op: 'NOT EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['ownerId'],
          childField: ['name'],
        },
        subquery: {
          table: 'users',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: '0001'},
          },
        },
      },
    },
  };

  const {ast: resolved} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => 'Alice',
  );

  expect((resolved.where as SimpleCondition).op).toBe('IS NOT');
});

test('returns ALWAYS_FALSE when executor returns undefined', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['ownerId'],
          childField: ['name'],
        },
        subquery: {
          table: 'users',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: 'nonexistent'},
          },
        },
      },
    },
  };

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => undefined,
  );

  expect(resolved.where).toEqual(ALWAYS_FALSE);
  expect(companions).toHaveLength(1);
});

test('returns ALWAYS_FALSE when executor returns null', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['ownerId'],
          childField: ['name'],
        },
        subquery: {
          table: 'users',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: '0001'},
          },
        },
      },
    },
  };

  const {ast: resolved} = resolveSimpleScalarSubqueries(ast, specs, () => null);

  expect(resolved.where).toEqual(ALWAYS_FALSE);
});

// NOT EXISTS is the negation, so an unresolvable subquery makes it always
// *true*: if no child row matches the subquery's WHERE, none can satisfy the
// correlation either, so the EXISTS is false for every parent row.
test('NOT EXISTS returns ALWAYS_TRUE when executor returns undefined', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'correlatedSubquery',
      op: 'NOT EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['ownerId'],
          childField: ['name'],
        },
        subquery: {
          table: 'users',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: 'nonexistent'},
          },
        },
      },
    },
  };

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => undefined,
  );

  expect(resolved.where).toEqual(ALWAYS_TRUE);
  // Still recorded, so the pipeline re-resolves if a matching row appears.
  expect(companions).toHaveLength(1);
});

test('NOT EXISTS returns ALWAYS_TRUE when executor returns null', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'correlatedSubquery',
      op: 'NOT EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['ownerId'],
          childField: ['name'],
        },
        subquery: {
          table: 'users',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: '0001'},
          },
        },
      },
    },
  };

  const {ast: resolved} = resolveSimpleScalarSubqueries(ast, specs, () => null);

  expect(resolved.where).toEqual(ALWAYS_TRUE);
});

test('leaves non-simple scalar subquery untouched', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['ownerId'],
          childField: ['name'],
        },
        subquery: {
          // No where clause → not simple
          table: 'users',
        },
      },
    },
  };

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => 'should not be called',
  );

  expect(resolved.where).toEqual(ast.where);
  expect(companions).toHaveLength(0);
});

test('resolves scalar subqueries inside AND conditions', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'closed'},
          right: {type: 'literal', value: false},
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          scalar: true,
          related: {
            correlation: {
              parentField: ['ownerId'],
              childField: ['id'],
            },
            subquery: {
              table: 'users',
              where: {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'id'},
                right: {type: 'literal', value: '0001'},
              },
            },
          },
        },
      ],
    },
  };

  const {ast: resolved} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => '0001',
  );

  expect(resolved.where).toEqual({
    type: 'and',
    conditions: [
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'closed'},
        right: {type: 'literal', value: false},
      },
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'ownerId'},
        right: {type: 'literal', value: '0001'},
      },
    ],
  });
});

test('resolves scalar subqueries inside OR conditions', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'or',
      conditions: [
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          scalar: true,
          related: {
            correlation: {
              parentField: ['ownerId'],
              childField: ['id'],
            },
            subquery: {
              table: 'users',
              where: {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'id'},
                right: {type: 'literal', value: '0001'},
              },
            },
          },
        },
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: '0003'},
        },
      ],
    },
  };

  const {ast: resolved} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => '0001',
  );

  expect(resolved.where).toEqual({
    type: 'or',
    conditions: [
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'ownerId'},
        right: {type: 'literal', value: '0001'},
      },
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'id'},
        right: {type: 'literal', value: '0003'},
      },
    ],
  });
});

test('resolves scalar subqueries in related subqueries', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    related: [
      {
        correlation: {
          parentField: ['ownerId'],
          childField: ['id'],
        },
        subquery: {
          table: 'users',
          where: {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            scalar: true,
            related: {
              correlation: {
                parentField: ['name'],
                childField: ['name'],
              },
              subquery: {
                table: 'users',
                where: {
                  type: 'simple',
                  op: '=',
                  left: {type: 'column', name: 'id'},
                  right: {type: 'literal', value: '0002'},
                },
              },
            },
          },
        },
      },
    ],
  };

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => 'Bob',
  );

  expect(resolved.related?.[0].subquery.where).toEqual({
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'name'},
    right: {type: 'literal', value: 'Bob'},
  });
  expect(companions).toHaveLength(1);
});

test('returns original AST when nothing to resolve', () => {
  const specs = makeTableSpecs({});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'id'},
      right: {type: 'literal', value: '1'},
    },
  };

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => undefined,
  );

  expect(resolved).toBe(ast);
  expect(companions).toHaveLength(0);
});

test('resolves scalar subqueries inside correlatedSubquery (EXISTS) conditions', () => {
  const specs = makeTableSpecs({
    label: [['id']],
  });

  const ast: AST = {
    table: 'issue',
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
        subquery: {
          table: 'issueLabel',
          where: {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            scalar: true,
            related: {
              correlation: {
                parentField: ['labelID'],
                childField: ['id'],
              },
              subquery: {
                table: 'label',
                where: {
                  type: 'simple',
                  op: '=',
                  left: {type: 'column', name: 'id'},
                  right: {type: 'literal', value: 'label-001'},
                },
              },
            },
          },
        },
      },
    },
  };

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => 'resolved-label-id',
  );

  // The outer correlatedSubquery condition should still be present, but its inner
  // subquery's WHERE should be resolved from scalar to simple.
  expect(resolved.where).toEqual({
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      correlation: {
        parentField: ['id'],
        childField: ['issueID'],
      },
      subquery: {
        table: 'issueLabel',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'labelID'},
          right: {type: 'literal', value: 'resolved-label-id'},
        },
      },
    },
  });
  expect(companions).toHaveLength(1);
  expect(companions[0].ast.table).toBe('label');
});

test('resolves scalar + flip on junction edge (issue -> issueLabel -> label)', () => {
  const specs = makeTableSpecs({
    label: [['id'], ['name']],
  });

  const inputAST = ast(
    newQuery(schema, 'issue').whereExists(
      'labels',
      q => q.where('name', 'foo'),
      {scalar: true, flip: true},
    ),
  );

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    inputAST,
    specs,
    () => 'label-foo-id',
  );

  // The inner scalar subquery is resolved to a literal condition,
  // and the outer EXISTS with flip: true is preserved.
  expect(resolved.where).toMatchObject({
    type: 'correlatedSubquery',
    op: 'EXISTS',
    flip: true,
    related: {
      system: 'client',
      correlation: {
        parentField: ['id'],
        childField: ['issueId'],
      },
      subquery: {
        table: 'issueLabel',
        alias: 'zsubq_labels',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'labelId'},
          right: {type: 'literal', value: 'label-foo-id'},
        },
      },
    },
  });
  expect(companions).toHaveLength(1);
  expect(companions[0].ast.table).toBe('label');
  expect(companions[0].childField).toBe('id');
  expect(companions[0].resolvedValue).toBe('label-foo-id');
});

test('resolves scalar subqueries inside AND of correlatedSubquery conditions', () => {
  const specs = makeTableSpecs({
    label: [['id']],
  });

  const scalarLabelCondition = (
    labelIdValue: string,
  ): CorrelatedSubqueryCondition => ({
    type: 'correlatedSubquery',
    op: 'EXISTS',
    scalar: true,
    related: {
      correlation: {
        parentField: ['labelID'],
        childField: ['id'],
      },
      subquery: {
        table: 'label',
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: labelIdValue},
        },
      },
    },
  });

  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            correlation: {
              parentField: ['id'],
              childField: ['issueID'],
            },
            subquery: {
              table: 'issueLabel',
              where: scalarLabelCondition('label-bug'),
            },
          },
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            correlation: {
              parentField: ['id'],
              childField: ['issueID'],
            },
            subquery: {
              table: 'issueLabel',
              where: scalarLabelCondition('label-software'),
            },
          },
        },
      ],
    },
  };

  const values: Record<string, string> = {
    'label-bug': 'bug-id',
    'label-software': 'software-id',
  };
  let callCount = 0;

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    (subAST, _field) => {
      callCount++;
      const idValue = (subAST.where as SimpleCondition).right as {
        type: 'literal';
        value: string;
      };
      return values[idValue.value];
    },
  );

  expect(callCount).toBe(2);
  expect(companions).toHaveLength(2);

  // Both correlatedSubquery conditions should have resolved inner WHERE
  const andCond = resolved.where as {type: 'and'; conditions: Condition[]};
  expect(andCond.type).toBe('and');

  for (const cond of andCond.conditions) {
    expect(cond.type).toBe('correlatedSubquery');
    if (cond.type === 'correlatedSubquery') {
      expect(cond.related.subquery.where?.type).toBe('simple');
    }
  }
});

test('leaves correlatedSubquery unchanged when inner subquery has no scalar subqueries', () => {
  const specs = makeTableSpecs({});

  const ast: AST = {
    table: 'issue',
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
        subquery: {
          table: 'issueLabel',
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'labelID'},
            right: {type: 'literal', value: 'some-id'},
          },
        },
      },
    },
  };

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => {
      throw new Error('should not be called');
    },
  );

  // Should return the original AST object (identity check)
  expect(resolved).toBe(ast);
  expect(companions).toHaveLength(0);
});

test('resolves nested scalar subqueries in subquery where clause', () => {
  const specs = makeTableSpecs({
    config: [['key']],
    users: [['id']],
  });

  const ast: AST = {
    table: 'issues',
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['ownerId'],
          childField: ['id'],
        },
        subquery: {
          table: 'users',
          where: {
            type: 'and',
            conditions: [
              {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'id'},
                right: {type: 'literal', value: '0001'},
              },
              {
                type: 'correlatedSubquery',
                op: 'EXISTS',
                scalar: true,
                related: {
                  correlation: {
                    parentField: ['role'],
                    childField: ['value'],
                  },
                  subquery: {
                    table: 'config',
                    where: {
                      type: 'simple',
                      op: '=',
                      left: {type: 'column', name: 'key'},
                      right: {type: 'literal', value: 'default_role'},
                    },
                  },
                },
              },
            ],
          },
        },
      },
    },
  };

  const values: Record<string, string> = {
    value: 'admin',
    id: '0001',
  };

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    (_subAST, field) => values[field],
  );

  // The inner scalar subquery (config lookup) should be resolved first,
  // then the outer one (users lookup) should also be resolved.
  expect(resolved.where).toEqual({
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'ownerId'},
    right: {type: 'literal', value: '0001'},
  });
  // Both the config and users subqueries become companions.
  expect(companions).toHaveLength(2);
});

// ---------- parent-literal propagation ----------

/**
 * The production shape this propagation exists for: the root is pinned to one
 * assignment by a literal, and the access gate correlates `assignment.id` to
 * that same column without repeating the literal. Without propagation the gate
 * is not provably single-row, so it runs once per tracker row.
 */
const TRACKER_SPECS = makeTableSpecs(
  {
    problem_tracker: [['id']],
    assignment: [['id']],
    teacher_assignment_access: [['id']],
  },
  {
    problem_tracker: {id: 'string', assignment_id: 'string'},
    assignment: {id: 'string', teacher_id: 'string'},
    teacher_assignment_access: {id: 'string', assignment_id: 'string'},
  },
);

const ASSIGNMENT_ID: SimpleCondition = {
  type: 'simple',
  op: '=',
  left: {type: 'column', name: 'assignment_id'},
  right: {type: 'literal', value: 'assignment_1'},
};

const PINNED_ASSIGNMENT: SimpleCondition = {
  type: 'simple',
  op: '=',
  left: {type: 'column', name: 'id'},
  right: {type: 'literal', value: 'assignment_1'},
};

/** `access grant OR I own it` — the real gate's body, with no literal on `id`. */
const ACCESS_GATE: Condition = {
  type: 'or',
  conditions: [
    {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['assignment_id']},
        subquery: {table: 'teacher_assignment_access'},
      },
    },
    {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'teacher_id'},
      right: {type: 'literal', value: 'teacher_1'},
    },
  ],
};

function scalarGate(subqueryWhere: Condition | undefined): Condition {
  return {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    scalar: true,
    related: {
      system: 'client',
      correlation: {parentField: ['assignment_id'], childField: ['id']},
      subquery: {
        table: 'assignment',
        alias: 'zsubq_assignment',
        where: subqueryWhere,
      },
    },
  };
}

const TRACKER_AST: AST = {
  table: 'problem_tracker',
  where: {type: 'and', conditions: [ASSIGNMENT_ID, scalarGate(ACCESS_GATE)]},
};

test('propagation: the production tracker access gate becomes eligible', () => {
  const executed: AST[] = [];
  const {
    ast: resolved,
    companions,
    ignoredScalarHints,
  } = resolveSimpleScalarSubqueries(TRACKER_AST, TRACKER_SPECS, sub => {
    executed.push(sub);
    return 'assignment_1';
  });

  expect(ignoredScalarHints).toEqual([]);
  expect(resolved.where).toEqual({
    type: 'and',
    conditions: [ASSIGNMENT_ID, ASSIGNMENT_ID],
  });

  // The subquery the executor ran — and the companion that keeps watching it —
  // is the access gate pinned to the one assignment the parent asked for.
  expect(executed).toEqual([
    {
      table: 'assignment',
      alias: 'zsubq_assignment',
      where: {type: 'and', conditions: [PINNED_ASSIGNMENT, ACCESS_GATE]},
      related: undefined,
    },
  ]);
  expect(companions).toEqual([
    {ast: executed[0], childField: 'id', resolvedValue: 'assignment_1'},
  ]);
});

test('propagation: is identical to writing the literal into the subquery', () => {
  const byHand: AST = {
    table: 'problem_tracker',
    where: {
      type: 'and',
      conditions: [
        ASSIGNMENT_ID,
        scalarGate({
          type: 'and',
          conditions: [PINNED_ASSIGNMENT, ACCESS_GATE],
        }),
      ],
    },
  };

  const execute = () => 'assignment_1';
  const propagated = resolveSimpleScalarSubqueries(
    TRACKER_AST,
    TRACKER_SPECS,
    execute,
  );
  const explicit = resolveSimpleScalarSubqueries(
    byHand,
    TRACKER_SPECS,
    execute,
  );

  expect(propagated).toEqual(explicit);
  expect(propagated.ignoredScalarHints).toEqual([]);
});

test('propagation: no matching row collapses the gate to false', () => {
  const {ast: resolved, ignoredScalarHints} = resolveSimpleScalarSubqueries(
    TRACKER_AST,
    TRACKER_SPECS,
    () => undefined,
  );

  expect(ignoredScalarHints).toEqual([]);
  expect(resolved.where).toEqual({
    type: 'and',
    conditions: [ASSIGNMENT_ID, ALWAYS_FALSE],
  });
});

test('propagation: NOT EXISTS resolves with IS NOT', () => {
  const gate = scalarGate(ACCESS_GATE) as CorrelatedSubqueryCondition;
  const ast: AST = {
    table: 'problem_tracker',
    where: {
      type: 'and',
      conditions: [ASSIGNMENT_ID, {...gate, op: 'NOT EXISTS'}],
    },
  };

  const {ast: resolved, ignoredScalarHints} = resolveSimpleScalarSubqueries(
    ast,
    TRACKER_SPECS,
    () => 'assignment_1',
  );

  expect(ignoredScalarHints).toEqual([]);
  expect(resolved.where).toEqual({
    type: 'and',
    conditions: [
      ASSIGNMENT_ID,
      {
        type: 'simple',
        op: 'IS NOT',
        left: {type: 'column', name: 'assignment_id'},
        right: {type: 'literal', value: 'assignment_1'},
      },
    ],
  });
});

test('propagation: the gate may sit under an OR as long as the literal does not', () => {
  // `pf = L` still applies to every row the query returns, so the equivalence
  // holds for a gate anywhere beneath it.
  const ast: AST = {
    table: 'problem_tracker',
    where: {
      type: 'and',
      conditions: [
        ASSIGNMENT_ID,
        {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'id'},
              right: {type: 'literal', value: 'tracker_1'},
            },
            scalarGate(ACCESS_GATE),
          ],
        },
      ],
    },
  };

  const {ast: resolved, ignoredScalarHints} = resolveSimpleScalarSubqueries(
    ast,
    TRACKER_SPECS,
    () => 'assignment_1',
  );

  expect(ignoredScalarHints).toEqual([]);
  expect(
    ((resolved.where as Conjunction).conditions[1] as Disjunction)
      .conditions[1],
  ).toEqual(ASSIGNMENT_ID);
});

test('propagation: completes a compound unique key with an in-subquery literal', () => {
  const specs = makeTableSpecs(
    {orders: [['id']], line_items: [['order_id', 'sku']]},
    {
      orders: {id: 'string', order_id: 'string'},
      line_items: {order_id: 'string', sku: 'string'},
    },
  );
  const ast: AST = {
    table: 'orders',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'order_id'},
          right: {type: 'literal', value: 'o1'},
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          scalar: true,
          related: {
            correlation: {parentField: ['order_id'], childField: ['order_id']},
            subquery: {
              table: 'line_items',
              where: {
                type: 'simple',
                op: '=',
                left: {type: 'column', name: 'sku'},
                right: {type: 'literal', value: 'widget'},
              },
            },
          },
        },
      ],
    },
  };

  const {companions, ignoredScalarHints} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => 'o1',
  );

  expect(ignoredScalarHints).toEqual([]);
  expect(companions).toHaveLength(1);
});

test('propagation: works for number and boolean literals', () => {
  const specs = makeTableSpecs(
    {events: [['id']], slots: [['slot'], ['active']]},
    {
      events: {id: 'string', slot: 'number', active: 'boolean'},
      slots: {slot: 'number', active: 'boolean'},
    },
  );
  for (const [column, value] of [
    ['slot', 7],
    ['active', true],
  ] as const) {
    const ast: AST = {
      table: 'events',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: column},
            right: {type: 'literal', value},
          },
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            scalar: true,
            related: {
              correlation: {parentField: [column], childField: [column]},
              subquery: {table: 'slots'},
            },
          },
        ],
      },
    };

    const {ignoredScalarHints, companions} = resolveSimpleScalarSubqueries(
      ast,
      specs,
      () => value,
    );
    expect(ignoredScalarHints).toEqual([]);
    expect(companions[0].ast.where).toEqual({
      type: 'simple',
      op: '=',
      left: {type: 'column', name: column},
      right: {type: 'literal', value},
    });
  }
});

// ---------- parent-literal propagation: shapes that must stay ineligible ----------

/**
 * Every case here would be wrong (or unproven) to propagate. Each asserts the
 * hint is *reported* as ignored rather than silently honored, which is what
 * the server warns about.
 */
function expectIneligible(ast: AST, specs = TRACKER_SPECS): void {
  const {
    ast: resolved,
    companions,
    ignoredScalarHints,
  } = resolveSimpleScalarSubqueries(ast, specs, () => {
    throw new Error('the executor must not run for an ineligible gate');
  });

  expect(companions).toEqual([]);
  expect(ignoredScalarHints).toHaveLength(1);
  expect(resolved).toEqual(ast);
}

test('ineligible: the literal is under an OR', () => {
  expectIneligible({
    table: 'problem_tracker',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'or',
          conditions: [
            ASSIGNMENT_ID,
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'assignment_id'},
              right: {type: 'literal', value: 'assignment_2'},
            },
          ],
        },
        scalarGate(ACCESS_GATE),
      ],
    },
  });
});

test('ineligible: the literal is inside a nested correlated subquery', () => {
  expectIneligible({
    table: 'problem_tracker',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            correlation: {parentField: ['id'], childField: ['assignment_id']},
            subquery: {
              table: 'teacher_assignment_access',
              where: ASSIGNMENT_ID,
            },
          },
        },
        scalarGate(ACCESS_GATE),
      ],
    },
  });
});

test('ineligible: the literal is under a NOT EXISTS', () => {
  expectIneligible({
    table: 'problem_tracker',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'correlatedSubquery',
          op: 'NOT EXISTS',
          related: {
            correlation: {parentField: ['id'], childField: ['assignment_id']},
            subquery: {
              table: 'teacher_assignment_access',
              where: ASSIGNMENT_ID,
            },
          },
        },
        scalarGate(ACCESS_GATE),
      ],
    },
  });
});

test('ineligible: the literal constrains a different column', () => {
  expectIneligible({
    table: 'problem_tracker',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: 'tracker_1'},
        },
        scalarGate(ACCESS_GATE),
      ],
    },
  });
});

test('ineligible: IN and inequality do not pin a single value', () => {
  for (const op of ['IN', '!=', '>', 'LIKE'] as const) {
    expectIneligible({
      table: 'problem_tracker',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op,
            left: {type: 'column', name: 'assignment_id'},
            right: {
              type: 'literal',
              value: op === 'IN' ? ['assignment_1'] : 'assignment_1',
            },
          },
          scalarGate(ACCESS_GATE),
        ],
      },
    });
  }
});

test('ineligible: the correlation covers only part of the unique key', () => {
  expectIneligible(
    {
      table: 'orders',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'order_id'},
            right: {type: 'literal', value: 'o1'},
          },
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            scalar: true,
            related: {
              correlation: {
                parentField: ['order_id'],
                childField: ['order_id'],
              },
              subquery: {table: 'line_items'},
            },
          },
        ],
      },
    },
    makeTableSpecs(
      {orders: [['id']], line_items: [['order_id', 'sku']]},
      {
        orders: {order_id: 'string'},
        line_items: {order_id: 'string', sku: 'string'},
      },
    ),
  );
});

test('ineligible: compound correlations, even when every column is pinned', () => {
  // The resolved gate can only name one parent column, so a compound
  // correlation cannot be answered by a single scalar.
  expectIneligible(
    {
      table: 'orders',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'order_id'},
            right: {type: 'literal', value: 'o1'},
          },
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'sku'},
            right: {type: 'literal', value: 'widget'},
          },
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            scalar: true,
            related: {
              correlation: {
                parentField: ['order_id', 'sku'],
                childField: ['order_id', 'sku'],
              },
              subquery: {table: 'line_items'},
            },
          },
        ],
      },
    },
    makeTableSpecs(
      {orders: [['id']], line_items: [['order_id', 'sku']]},
      {
        orders: {order_id: 'string', sku: 'string'},
        line_items: {order_id: 'string', sku: 'string'},
      },
    ),
  );
});

test('ineligible: the parent and child columns have different types', () => {
  expectIneligible(
    TRACKER_AST,
    makeTableSpecs(
      {problem_tracker: [['id']], assignment: [['id']]},
      {
        problem_tracker: {assignment_id: 'string'},
        assignment: {id: 'number'},
      },
    ),
  );
});

test('ineligible: the literal does not match the declared column type', () => {
  expectIneligible(
    TRACKER_AST,
    makeTableSpecs(
      {problem_tracker: [['id']], assignment: [['id']]},
      {
        problem_tracker: {assignment_id: 'number'},
        assignment: {id: 'number'},
      },
    ),
  );
});

test('ineligible: json columns encode differently on the two paths', () => {
  expectIneligible(
    TRACKER_AST,
    makeTableSpecs(
      {problem_tracker: [['id']], assignment: [['id']]},
      {
        problem_tracker: {assignment_id: 'json'},
        assignment: {id: 'json'},
      },
    ),
  );
});

test('ineligible: null and array literals', () => {
  for (const value of [null, ['assignment_1']] as const) {
    expectIneligible({
      table: 'problem_tracker',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'assignment_id'},
            right: {type: 'literal', value},
          },
          scalarGate(ACCESS_GATE),
        ],
      },
    });
  }
});

test('ineligible: the column types are not known', () => {
  expectIneligible(
    TRACKER_AST,
    makeTableSpecs({problem_tracker: [['id']], assignment: [['id']]}),
  );
});

test('ineligible: a literal in an outer query does not reach a related subquery', () => {
  // `assignment_id` is a column of `problem_tracker`, not of
  // `teacher_assignment_access`; scoping the literals to one AST is what keeps
  // them from leaking down.
  expectIneligible({
    table: 'problem_tracker',
    where: ASSIGNMENT_ID,
    related: [
      {
        correlation: {parentField: ['id'], childField: ['assignment_id']},
        subquery: {
          table: 'teacher_assignment_access',
          where: scalarGate(ACCESS_GATE),
        },
      },
    ],
  });
});
