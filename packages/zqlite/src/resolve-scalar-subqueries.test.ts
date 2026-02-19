import {expect, test} from 'vitest';
import type {
  AST,
  Condition,
  SimpleCondition,
} from '../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.ts';
import {
  extractLiteralEqualityConstraints,
  isSimpleSubquery,
  resolveSimpleScalarSubqueries,
} from './resolve-scalar-subqueries.ts';

function makeTableSpecs(
  entries: Record<string, PrimaryKey[]>,
): Map<string, {tableSpec: {uniqueKeys: PrimaryKey[]}}> {
  const map = new Map<string, {tableSpec: {uniqueKeys: PrimaryKey[]}}>();
  for (const [table, uniqueKeys] of Object.entries(entries)) {
    map.set(table, {tableSpec: {uniqueKeys}});
  }
  return map;
}

const ALWAYS_FALSE: SimpleCondition = {
  type: 'simple',
  op: '=',
  left: {type: 'literal', value: 1},
  right: {type: 'literal', value: 0},
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
      type: 'scalarSubquery',
      op: '=',
      parentField: 'ownerId',
      childField: 'name',
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

test('preserves the operator from the scalar subquery condition', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'scalarSubquery',
      op: 'IS NOT',
      parentField: 'ownerId',
      childField: 'name',
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
      type: 'scalarSubquery',
      op: '=',
      parentField: 'ownerId',
      childField: 'name',
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
      type: 'scalarSubquery',
      op: '=',
      parentField: 'ownerId',
      childField: 'name',
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
  };

  const {ast: resolved} = resolveSimpleScalarSubqueries(ast, specs, () => null);

  expect(resolved.where).toEqual(ALWAYS_FALSE);
});

test('leaves non-simple scalar subquery untouched', () => {
  const specs = makeTableSpecs({users: [['id']]});
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'scalarSubquery',
      op: '=',
      parentField: 'ownerId',
      childField: 'name',
      subquery: {
        // No where clause → not simple
        table: 'users',
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
          type: 'scalarSubquery',
          op: '=',
          parentField: 'ownerId',
          childField: 'id',
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
          type: 'scalarSubquery',
          op: '=',
          parentField: 'ownerId',
          childField: 'id',
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
            type: 'scalarSubquery',
            op: '=',
            parentField: 'name',
            childField: 'name',
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
            type: 'scalarSubquery',
            op: '=',
            parentField: 'labelID',
            childField: 'id',
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
  };

  const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
    ast,
    specs,
    () => 'resolved-label-id',
  );

  // The correlatedSubquery condition should still be present, but its inner
  // subquery's WHERE should be resolved from scalarSubquery to simple.
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

test('resolves scalar subqueries inside AND of correlatedSubquery conditions', () => {
  const specs = makeTableSpecs({
    label: [['id']],
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
              where: {
                type: 'scalarSubquery',
                op: '=',
                parentField: 'labelID',
                childField: 'id',
                subquery: {
                  table: 'label',
                  where: {
                    type: 'simple',
                    op: '=',
                    left: {type: 'column', name: 'id'},
                    right: {type: 'literal', value: 'label-bug'},
                  },
                },
              },
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
              where: {
                type: 'scalarSubquery',
                op: '=',
                parentField: 'labelID',
                childField: 'id',
                subquery: {
                  table: 'label',
                  where: {
                    type: 'simple',
                    op: '=',
                    left: {type: 'column', name: 'id'},
                    right: {type: 'literal', value: 'label-software'},
                  },
                },
              },
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
      type: 'scalarSubquery',
      op: '=',
      parentField: 'ownerId',
      childField: 'id',
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
              type: 'scalarSubquery',
              op: '=',
              parentField: 'role',
              childField: 'value',
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
          ],
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
