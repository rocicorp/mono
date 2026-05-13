import {describe, expect, test} from 'vitest';
import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';
import {createBuilder} from '../../../../zql/src/query/create-builder.ts';
import {asQueryInternals} from '../../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {schema} from '../../../../zql/src/query/test/test-schemas.ts';
import {covers} from './covers.ts';

const b = createBuilder(schema);

function ast(q: AnyQuery): AST {
  return asQueryInternals(q).ast;
}

describe('covers - basic structure', () => {
  test('identical empty queries cover each other', () => {
    expect(covers(ast(b.issue), ast(b.issue))).toBe(true);
  });

  test('different tables do not cover', () => {
    expect(covers(ast(b.issue), ast(b.comment))).toBe(false);
    expect(covers(ast(b.comment), ast(b.issue))).toBe(false);
  });
});

describe('covers - WHERE inclusion', () => {
  test('no where covers any where', () => {
    const a = ast(b.issue);
    const r = ast(b.issue.where('id', 'x'));
    expect(covers(a, r)).toBe(true);
    expect(covers(r, a)).toBe(false);
  });

  test('same where → mutual coverage', () => {
    const x = ast(b.issue.where('closed', false));
    const y = ast(b.issue.where('closed', false));
    expect(covers(x, y)).toBe(true);
    expect(covers(y, x)).toBe(true);
  });

  test('A conjuncts ⊆ B conjuncts → A covers B', () => {
    const a = ast(b.issue.where('closed', false));
    const r = ast(b.issue.where('closed', false).where('ownerId', 'matt'));
    expect(covers(a, r)).toBe(true);
    expect(covers(r, a)).toBe(false);
  });

  test('conjunct order does not matter (normalization sorts)', () => {
    const x = ast(b.issue.where('closed', false).where('ownerId', 'matt'));
    const y = ast(b.issue.where('ownerId', 'matt').where('closed', false));
    expect(covers(x, y)).toBe(true);
    expect(covers(y, x)).toBe(true);
  });

  test('different values do not cover', () => {
    const x = ast(b.issue.where('closed', false));
    const y = ast(b.issue.where('closed', true));
    expect(covers(x, y)).toBe(false);
    expect(covers(y, x)).toBe(false);
  });

  test('an OR in both, structurally equal → mutual coverage', () => {
    const x = ast(
      b.issue.where(({or, cmp}) =>
        or(cmp('closed', false), cmp('closed', true)),
      ),
    );
    // Same disjuncts in opposite order — normalizeAST sorts them.
    const y = ast(
      b.issue.where(({or, cmp}) =>
        or(cmp('closed', true), cmp('closed', false)),
      ),
    );
    expect(covers(x, y)).toBe(true);
    expect(covers(y, x)).toBe(true);
  });
});

describe('covers - limit / start / orderBy', () => {
  test('A unlimited covers B with any limit', () => {
    const a = ast(b.issue);
    const r = ast(b.issue.orderBy('id', 'asc').limit(100));
    expect(covers(a, r)).toBe(true);
  });

  test('A limited does not cover B unlimited', () => {
    const a = ast(b.issue.orderBy('id', 'asc').limit(100));
    const r = ast(b.issue);
    expect(covers(a, r)).toBe(false);
  });

  test('A and B have matching limit/orderBy → covers', () => {
    const a = ast(b.issue.orderBy('id', 'asc').limit(100));
    const r = ast(
      b.issue.where('closed', false).orderBy('id', 'asc').limit(100),
    );
    expect(covers(a, r)).toBe(true);
  });

  test('different limits → no cover', () => {
    const x = ast(b.issue.orderBy('id', 'asc').limit(100));
    const y = ast(b.issue.orderBy('id', 'asc').limit(50));
    expect(covers(x, y)).toBe(false);
    expect(covers(y, x)).toBe(false);
  });

  test('different orderBy with limit → no cover', () => {
    const x = ast(b.issue.orderBy('id', 'asc').limit(10));
    const y = ast(b.issue.orderBy('id', 'desc').limit(10));
    expect(covers(x, y)).toBe(false);
  });

  test('orderBy without limit/start is ignored', () => {
    const x = ast(b.issue.orderBy('id', 'asc'));
    const y = ast(b.issue.orderBy('id', 'desc'));
    expect(covers(x, y)).toBe(true);
    expect(covers(y, x)).toBe(true);
  });

  test('different start bounds with same limit → no cover', () => {
    const x = ast(b.issue.orderBy('id', 'asc').start({id: 'a'}).limit(10));
    const y = ast(b.issue.orderBy('id', 'asc').start({id: 'b'}).limit(10));
    expect(covers(x, y)).toBe(false);
  });
});

describe('covers - related subqueries', () => {
  test('A has related that B has → covers', () => {
    const x = ast(b.issue.related('comments'));
    const y = ast(b.issue.related('comments'));
    expect(covers(x, y)).toBe(true);
    expect(covers(y, x)).toBe(true);
  });

  test('A has related, B has no related → A covers B', () => {
    const a = ast(b.issue.related('comments'));
    const r = ast(b.issue);
    expect(covers(a, r)).toBe(true);
  });

  test('B has related, A does not → no cover', () => {
    const a = ast(b.issue);
    const r = ast(b.issue.related('comments'));
    expect(covers(a, r)).toBe(false);
  });

  test('A has superset of relateds → covers', () => {
    const a = ast(b.issue.related('comments').related('owner'));
    const r = ast(b.issue.related('comments'));
    expect(covers(a, r)).toBe(true);
    expect(covers(r, a)).toBe(false);
  });

  test('A.related subquery covers B.related subquery (less restrictive)', () => {
    const a = ast(b.issue.related('comments'));
    const r = ast(
      b.issue.related('comments', cq => cq.where('authorId', 'matt')),
    );
    expect(covers(a, r)).toBe(true);
    expect(covers(r, a)).toBe(false);
  });

  test('different relationships → no cover', () => {
    const a = ast(b.issue.related('comments'));
    const r = ast(b.issue.related('owner'));
    expect(covers(a, r)).toBe(false);
    expect(covers(r, a)).toBe(false);
  });

  test('junction relationships (labels) round-trip', () => {
    const x = ast(b.issue.related('labels'));
    const y = ast(b.issue.related('labels'));
    expect(covers(x, y)).toBe(true);
    expect(covers(y, x)).toBe(true);
  });
});

describe('covers - EXISTS conjuncts', () => {
  test('identical EXISTS in both → mutual coverage', () => {
    const x = ast(b.issue.whereExists('comments'));
    const y = ast(b.issue.whereExists('comments'));
    expect(covers(x, y)).toBe(true);
    expect(covers(y, x)).toBe(true);
  });

  test('EXISTS in A but not in B → A is more restrictive (no cover)', () => {
    const a = ast(b.issue.whereExists('comments'));
    const r = ast(b.issue);
    expect(covers(a, r)).toBe(false);
    expect(covers(r, a)).toBe(true);
  });

  test('different EXISTS subqueries → no cover', () => {
    const x = ast(
      b.issue.whereExists('comments', cq => cq.where('authorId', 'matt')),
    );
    const y = ast(
      b.issue.whereExists('comments', cq => cq.where('authorId', 'alice')),
    );
    expect(covers(x, y)).toBe(false);
    expect(covers(y, x)).toBe(false);
  });
});

describe('covers - static parameters', () => {
  // The query builder doesn't surface static parameters directly (they're
  // injected by the permission system at bind time). Constructed by hand to
  // verify the algorithm handles them.
  test('identical static-param condition matches', () => {
    const param: Condition = {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'ownerId'},
      right: {type: 'static', anchor: 'authData', field: 'userId'},
    };
    const a: AST = {table: 'issue', where: param};
    const r: AST = {
      table: 'issue',
      where: {
        type: 'and',
        conditions: [
          param,
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'closed'},
            right: {type: 'literal', value: false},
          },
        ],
      },
    };
    expect(covers(a, r)).toBe(true);
  });

  test('different anchors do not match', () => {
    const aWhere: Condition = {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'ownerId'},
      right: {type: 'static', anchor: 'authData', field: 'userId'},
    };
    const bWhere: Condition = {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'ownerId'},
      right: {type: 'static', anchor: 'preMutationRow', field: 'userId'},
    };
    expect(
      covers({table: 'issue', where: aWhere}, {table: 'issue', where: bWhere}),
    ).toBe(false);
  });
});

describe('covers - v1 deliberately rejects (semantic implication)', () => {
  test('range subsumption not detected', () => {
    // Semantically: createdAt > 5 admits everything createdAt > 10 admits.
    const x = ast(b.issue.where('createdAt', '>', 5));
    const y = ast(b.issue.where('createdAt', '>', 10));
    expect(covers(x, y)).toBe(false);
  });

  test('OR widening not detected', () => {
    // Semantically: (closed=false OR closed=true) admits everything closed=false admits.
    const a = ast(
      b.issue.where(({or, cmp}) =>
        or(cmp('closed', false), cmp('closed', true)),
      ),
    );
    const r = ast(b.issue.where('closed', false));
    expect(covers(a, r)).toBe(false);
  });

  test('single conjunct vs and-wrapped: handled by flattening', () => {
    // normalizeAST flattens singleton and's, so a single .where() and an or()
    // wrapping a single cmp resolve to the same form.
    const x = ast(b.issue.where('closed', false));
    const y = ast(b.issue.where(({cmp, and}) => and(cmp('closed', false))));
    expect(covers(x, y)).toBe(true);
    expect(covers(y, x)).toBe(true);
  });
});

describe('covers - realistic patterns', () => {
  test('full-table subscribe covers any single-row point query', () => {
    const all = ast(b.issue);
    const point = ast(b.issue.where('id', 'abc123'));
    expect(covers(all, point)).toBe(true);
  });

  test('full-table subscribe covers filtered query with related', () => {
    const all = ast(b.issue.related('comments'));
    const filtered = ast(b.issue.where('closed', false).related('comments'));
    expect(covers(all, filtered)).toBe(true);
  });

  test('two unrelated queries do not cover each other', () => {
    const x = ast(b.issue.where('ownerId', 'alice'));
    const y = ast(b.issue.where('ownerId', 'bob'));
    expect(covers(x, y)).toBe(false);
    expect(covers(y, x)).toBe(false);
  });
});
