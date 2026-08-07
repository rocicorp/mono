import {expect, test} from 'vitest';
import {MAX_AST_DEPTH, assertAstDepth} from './ast.ts';

function buildAndChain(depth: number) {
  let cond: unknown = {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'id'},
    right: {type: 'literal', value: 'x'},
  };
  for (let i = 0; i < depth; i++) {
    cond = {type: 'and', conditions: [cond]};
  }
  return cond;
}

function buildExistsChain(depth: number) {
  // Nests through correlatedSubquery -> AST -> where -> correlatedSubquery
  // -> ... to exercise the mutually-recursive Condition/AST axis.
  let ast: Record<string, unknown> = {
    table: 'leaf',
  };
  for (let i = 0; i < depth; i++) {
    const cond = {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        correlation: {parentField: ['id'], childField: ['parentId']},
        subquery: ast,
      },
    };
    ast = {
      table: `outer_${i}`,
      where: cond,
    };
  }
  return ast;
}

function buildRelatedChain(depth: number) {
  // Nests AST.related[].subquery -> AST -> related[].subquery to exercise
  // the other recursive axis.
  let ast: Record<string, unknown> = {
    table: 'leaf',
  };
  for (let i = 0; i < depth; i++) {
    ast = {
      table: `outer_${i}`,
      related: [
        {
          correlation: {parentField: ['id'], childField: ['parentId']},
          subquery: ast,
        },
      ],
    };
  }
  return ast;
}

test('assertAstDepth accepts shallow ASTs', () => {
  const ast = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: 'x'},
        },
        {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              op: '=',
              left: {type: 'column', name: 'id'},
              right: {type: 'literal', value: 'y'},
            },
          ],
        },
      ],
    },
  };
  expect(() => assertAstDepth(ast)).not.toThrow();
});

test('assertAstDepth accepts depth at the limit', () => {
  const ast = {
    table: 't',
    where: buildAndChain(MAX_AST_DEPTH),
  };
  // The wrapping AST + the first AND step both contribute, so the chain
  // generated above tops out at exactly MAX_AST_DEPTH when measured from
  // the AST root. Specifically: each `and` is a +1, applied
  // MAX_AST_DEPTH times.
  expect(() => assertAstDepth(ast)).not.toThrow();
});

test('assertAstDepth rejects a deeply-nested AND chain', () => {
  const ast = {
    table: 't',
    where: buildAndChain(MAX_AST_DEPTH + 5),
  };
  expect(() => assertAstDepth(ast)).toThrow(/depth/);
});

test('assertAstDepth rejects depth=1000 in well under 100ms', () => {
  const ast = {
    table: 't',
    where: buildAndChain(1000),
  };
  const start = performance.now();
  expect(() => assertAstDepth(ast)).toThrow(/depth/);
  const elapsed = performance.now() - start;
  // Single iterative pass over ~1000 nodes; comfortably sub-millisecond
  // in practice. Give ourselves a generous budget to avoid CI flake.
  expect(elapsed).toBeLessThan(100);
});

test('assertAstDepth catches the mutually-recursive EXISTS axis', () => {
  // Each iteration adds: condition -> correlatedSubquery -> ast -> where
  // -> condition. That should overflow MAX_AST_DEPTH at roughly the same
  // chain length as the AND-only chain.
  const ast = buildExistsChain(MAX_AST_DEPTH + 5);
  expect(() => assertAstDepth(ast)).toThrow(/depth/);
});

test('assertAstDepth catches the AST.related[] subquery axis', () => {
  const ast = buildRelatedChain(MAX_AST_DEPTH + 5);
  expect(() => assertAstDepth(ast)).toThrow(/depth/);
});

test('assertAstDepth tolerates malformed/unknown shapes', () => {
  // Anything that isn't an AST-shaped object is left for valita to reject.
  expect(() => assertAstDepth(null)).not.toThrow();
  expect(() => assertAstDepth(undefined)).not.toThrow();
  expect(() => assertAstDepth('not an ast')).not.toThrow();
  expect(() => assertAstDepth(42)).not.toThrow();
  expect(() => assertAstDepth({table: 't', where: 'oops'})).not.toThrow();
  expect(() =>
    assertAstDepth({table: 't', related: 'not an array'}),
  ).not.toThrow();
});

test('assertAstDepth respects a custom limit', () => {
  const ast = {table: 't', where: buildAndChain(5)};
  expect(() => assertAstDepth(ast, 3)).toThrow(/depth/);
  expect(() => assertAstDepth(ast, 10)).not.toThrow();
});

test('assertAstDepth rejects depth 1000 within 100ms', () => {
  const ast = {table: 't', where: buildAndChain(1000)};
  const start = performance.now();
  expect(() => assertAstDepth(ast)).toThrow(/depth/);
  const elapsed = performance.now() - start;
  expect(elapsed).toBeLessThan(100);
});
