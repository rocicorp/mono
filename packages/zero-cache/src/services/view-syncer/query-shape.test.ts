import {expect, test} from 'vitest';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {queryShape} from './query-shape.ts';

function issuesByProject(projectID: string, titleLike: string): AST {
  return {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'projectID'},
          op: '=',
          right: {type: 'literal', value: projectID},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'title'},
          op: 'ILIKE',
          right: {type: 'literal', value: titleLike},
        },
      ],
    },
    orderBy: [
      ['modified', 'desc'],
      ['id', 'asc'],
    ],
    limit: 100,
  };
}

test('redacts literals', () => {
  expect(queryShape(issuesByProject('p1', '%secret%')).zql).toBe(
    `issue.where('projectID', ?).where('title', 'ILIKE', ?)` +
      `.orderBy('modified', 'desc').orderBy('id', 'asc').limit(100)`,
  );
});

test('queries that differ only in literals share a shape', () => {
  const a = queryShape(issuesByProject('p1', '%foo%'));
  const b = queryShape(issuesByProject('p2', '%bar%'));
  expect(a).toEqual(b);
  expect(a.hash).toMatch(/^[0-9a-z]+$/);
});

test('queries that differ in structure have different shapes', () => {
  const a = issuesByProject('p1', '%foo%');
  const b: AST = {...a, limit: 10};
  const c: AST = {...a, table: 'comment'};
  const hashes = new Set([a, b, c].map(ast => queryShape(ast).hash));
  expect(hashes.size).toBe(3);
});
