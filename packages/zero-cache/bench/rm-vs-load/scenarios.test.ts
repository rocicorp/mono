import {expect, test} from 'vitest';
import {EnvReader} from './config.ts';
import {describeScenarios, loadScenarios} from './scenarios.ts';

test('loads the smoke scenario by default', () => {
  const scenarios = loadScenarios({full: false, env: new EnvReader({})});

  expect(scenarios.map(scenario => scenario.name)).toEqual(['steady-small']);
  expect(scenarios[0].targetTxPerSec).toBe(500);
  expect(describeScenarios(scenarios)).toBe('steady-small=1x96 B insert-only');
});

test('loads full scenarios with env backed targets and mixed weights', () => {
  const scenarios = loadScenarios({
    full: true,
    env: new EnvReader({
      ZERO_RM_VS_MEDIUM_WIDE_TARGET_TPS: '4321',
      ZERO_RM_VS_SHARED_SYNC_TARGET_TPS: '2345',
      ZERO_RM_VS_SYNC_WORKERS: '6',
      ZERO_RM_VS_MIXED_INSERT_WEIGHT: '5',
      ZERO_RM_VS_MIXED_UPDATE_WEIGHT: '3',
      ZERO_RM_VS_MIXED_DELETE_WEIGHT: '2',
    }),
  });

  const mediumWide = scenarios.find(
    scenario => scenario.name === 'medium-wide-batch-pressure',
  );
  const mixed = scenarios.find(
    scenario => scenario.name === 'mixed-hot-row-churn',
  );
  const shared = scenarios.find(
    scenario => scenario.name === 'shared-replica-sync-workers',
  );

  expect(mediumWide?.targetTxPerSec).toBe(4321);
  expect(shared?.targetTxPerSec).toBe(2345);
  expect(shared?.syncWorkerCount).toBe(6);
  expect(mixed?.workload).toEqual({
    kind: 'mixed-row-churn',
    insertWeight: 5,
    updateWeight: 3,
    deleteWeight: 2,
  });
});

test('filters scenarios by exact name and reports valid choices', () => {
  const scenarios = loadScenarios({
    full: true,
    env: new EnvReader({
      ZERO_RM_VS_SCENARIO: 'mixed-hot-row-churn',
    }),
  });

  expect(scenarios.map(scenario => scenario.name)).toEqual([
    'mixed-hot-row-churn',
  ]);
  expect(() =>
    loadScenarios({
      full: true,
      env: new EnvReader({ZERO_RM_VS_SCENARIO: 'missing'}),
    }),
  ).toThrow(/Unknown ZERO_RM_VS_SCENARIO=missing/);
});
