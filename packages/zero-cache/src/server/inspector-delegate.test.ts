import {expect, test} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {InspectorDelegate} from './inspector-delegate.ts';

test('routes one query meta data to all queries sharing a transformationHash', () => {
  const d = new InspectorDelegate();

  const hash = 'same-xform-hash';
  const q1 = 'query-A';
  const q2 = 'query-B';
  const ast: AST = {table: 'issues'};

  d.addQuery(hash, q1, ast);
  d.addQuery(hash, q2, ast);

  // Emit metrics for that transformation
  d.addMetric('query-update-server', 10, hash);
  d.addMetric('query-materialization-server', 7, hash);

  const m1 = d.getMetricsJSONForQuery(q1);
  const m2 = d.getMetricsJSONForQuery(q2);

  expect(m1).toEqual({
    'query-materialization-server': [1000, 7, 1],
    'query-update-server': [1000, 10, 1],
  });

  expect(m1).toEqual(m2);

  expect(d.getASTForQuery(q1)).toEqual(ast);
  expect(d.getASTForQuery(q2)).toEqual(ast);
});
