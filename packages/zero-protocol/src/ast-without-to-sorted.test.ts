import {expect, test} from 'vitest';
import {normalizeAST, type AST} from './ast.ts';

test('normalizeAST sorts related without Array.prototype.toSorted', () => {
  // Hermes, the React Native JS engine, does not implement it.
  const toSorted = Object.getOwnPropertyDescriptor(Array.prototype, 'toSorted');
  Reflect.deleteProperty(Array.prototype, 'toSorted');
  try {
    const ast: AST = {
      table: 'issue',
      related: [
        {
          correlation: {parentField: ['creatorID'], childField: ['id']},
          subquery: {table: 'user', alias: 'creator'},
        },
        {
          correlation: {parentField: ['id'], childField: ['issueID']},
          subquery: {table: 'comment', alias: 'comments'},
        },
      ],
    };
    expect(normalizeAST(ast).related?.map(r => r.subquery.alias)).toEqual([
      'comments',
      'creator',
    ]);
  } finally {
    if (toSorted) {
      Object.defineProperty(Array.prototype, 'toSorted', toSorted);
    }
  }
});
