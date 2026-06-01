import {expect, test} from 'vitest';
import {smokePayloadProfiles} from './fixtures.ts';
import type {Scenario} from './types.ts';
import {createTransactionGenerator, workloadName} from './workloads.ts';

const smallPayload = smokePayloadProfiles[0];

test('insert only workload generates one transaction envelope with row inserts', () => {
  const scenario: Scenario = {
    name: 'insert-test',
    rowsPerTx: 3,
    payload: smallPayload,
    targetTxPerSec: 100,
    workload: {kind: 'insert-only'},
  };

  const tx = createTransactionGenerator(scenario).next(7);

  expect(tx.watermark).toBe('000000000007');
  expect(tx.rows).toBe(3);
  expect(tx.operationCounts).toEqual({insert: 3, update: 0, delete: 0});
  expect(tx.changes.map(change => change[0])).toEqual([
    'begin',
    'data',
    'data',
    'data',
    'commit',
  ]);
});

test('mixed row churn follows the configured operation cadence', () => {
  const scenario: Scenario = {
    name: 'mixed-test',
    rowsPerTx: 10,
    payload: smallPayload,
    targetTxPerSec: 100,
    workload: {
      kind: 'mixed-row-churn',
      insertWeight: 4,
      updateWeight: 4,
      deleteWeight: 2,
    },
  };
  const generator = createTransactionGenerator(scenario);

  const first = generator.next(1);
  const second = generator.next(2);

  expect(first.operationCounts).toEqual({insert: 4, update: 4, delete: 2});
  expect(second.operationCounts).toEqual({insert: 4, update: 4, delete: 2});
  expect(first.changes.at(0)?.[0]).toBe('begin');
  expect(first.changes.at(-1)?.[0]).toBe('commit');
});

test('mixed row churn inserts until an update or delete has an active row', () => {
  const scenario: Scenario = {
    name: 'delete-heavy',
    rowsPerTx: 4,
    payload: smallPayload,
    targetTxPerSec: 100,
    workload: {
      kind: 'mixed-row-churn',
      insertWeight: 0,
      updateWeight: 0,
      deleteWeight: 1,
    },
  };

  const tx = createTransactionGenerator(scenario).next(1);

  expect(tx.operationCounts).toEqual({insert: 2, update: 0, delete: 2});
});

test('workload names are stable for benchmark summaries', () => {
  expect(workloadName({kind: 'insert-only'})).toBe('insert-only');
  expect(
    workloadName({
      kind: 'mixed-row-churn',
      insertWeight: 4,
      updateWeight: 4,
      deleteWeight: 2,
    }),
  ).toBe('mixed-row-churn 4i/4u/2d');
});
