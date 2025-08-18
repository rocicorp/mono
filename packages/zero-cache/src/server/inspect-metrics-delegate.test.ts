import {expect, test} from 'vitest';
import {InspectMetricsDelegate} from './inspect-metrics-delegate.ts';

test('routes one server metric update to all queries sharing a transformationHash', () => {
  const d = new InspectMetricsDelegate();

  const hash = 'same-xform-hash';
  const q1 = 'query-A';
  const q2 = 'query-B';

  d.addQueryMapping(hash, q1);
  d.addQueryMapping(hash, q2);

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
});
