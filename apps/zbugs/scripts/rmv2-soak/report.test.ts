import {expect, test} from 'vitest';
import {requiredRouteCoverage} from './report.ts';

test('requires only routes exercised by the selected chaos actions', () => {
  const coverage = requiredRouteCoverage(
    {'sqlite/selected': 3, 'sqlite/selected-cold': 1},
    ['C1', 'C6'],
  );

  expect(coverage).toEqual([
    {
      route: 'sqlite/selected',
      count: 3,
      triggeredBy: 'C1, C2, or C4',
    },
    {
      route: 'sqlite/selected-cold',
      count: 1,
      triggeredBy: 'C6 or C13 after a change-log reseed',
    },
  ]);
});

test('does not require demotion routes after the backup coverage fix', () => {
  const coverage = requiredRouteCoverage({}, ['C6', 'C13']);

  expect(coverage.map(({route}) => route)).toEqual(['sqlite/selected-cold']);
  expect(coverage[0]?.count).toBe(0);
});

test('has no required routes when chaos is disabled', () => {
  expect(requiredRouteCoverage({}, [])).toEqual([]);
});

test('requires the PG cold-log route when cold reads are disabled', () => {
  expect(requiredRouteCoverage({'pg/cold-log': 3}, ['C6'], 0)).toEqual([
    {
      route: 'pg/cold-log',
      count: 3,
      triggeredBy: 'C6 or C13 after a reseed with cold reads disabled',
    },
  ]);
});

test('accepts either cold route for a partial rollout', () => {
  expect(
    requiredRouteCoverage(
      {'pg/cold-log': 2, 'sqlite/selected-cold': 1},
      ['C13'],
      50,
    ),
  ).toEqual([
    {
      route: 'sqlite/selected-cold or pg/cold-log',
      count: 3,
      triggeredBy: 'C6 or C13 with partial cold-read sampling',
    },
  ]);
});
