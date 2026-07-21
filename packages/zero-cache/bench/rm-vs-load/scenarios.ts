import {EnvReader} from './config.ts';
import {
  loadPayloadProfiles,
  smokePayloadProfiles,
  type PayloadProfile,
} from './fixtures.ts';
import {formatBytes} from './perf-utils.ts';
import type {Scenario} from './types.ts';
import {workloadName} from './workloads.ts';

export type ScenarioCatalogOptions = {
  readonly full: boolean;
  readonly env?: EnvReader | undefined;
};

export function loadScenarios({
  full,
  env = new EnvReader(),
}: ScenarioCatalogOptions): Scenario[] {
  return filterScenarios(full ? fullScenarios(env) : smokeScenarios(env), env);
}

export function describeScenarios(scenarios: readonly Scenario[]): string {
  return scenarios
    .map(
      s =>
        `${s.name}=${s.rowsPerTx}x${formatBytes(s.payload.bytes)} ` +
        workloadName(s.workload) +
        (s.syncWorkerCount === undefined
          ? ''
          : ` sync-workers=${s.syncWorkerCount}`),
    )
    .join(', ');
}

function smokeScenarios(env: EnvReader): Scenario[] {
  const small = profile('small', smokePayloadProfiles);
  return [
    {
      name: 'steady-small',
      rowsPerTx: 1,
      payload: small,
      targetTxPerSec: env.int('ZERO_RM_VS_TARGET_TPS', 500),
      workload: {kind: 'insert-only'},
    },
  ];
}

function fullScenarios(env: EnvReader): Scenario[] {
  const small = profile('small', loadPayloadProfiles);
  const medium = profile('medium', loadPayloadProfiles);
  const large = profile('large', loadPayloadProfiles);
  return [
    {
      name: 'single-row-flood',
      rowsPerTx: 1,
      payload: small,
      targetTxPerSec: env.int('ZERO_RM_VS_SMALL_TARGET_TPS', 1_200),
      workload: {kind: 'insert-only'},
    },
    {
      name: 'medium-batch-pressure',
      rowsPerTx: 10,
      payload: medium,
      targetTxPerSec: env.int('ZERO_RM_VS_MEDIUM_TARGET_TPS', 400),
      workload: {kind: 'insert-only'},
    },
    {
      name: 'medium-wide-batch-pressure',
      rowsPerTx: 20,
      payload: medium,
      targetTxPerSec: env.int('ZERO_RM_VS_MEDIUM_WIDE_TARGET_TPS', 1_000),
      workload: {kind: 'insert-only'},
    },
    {
      name: 'shared-replica-sync-workers',
      rowsPerTx: 20,
      payload: medium,
      targetTxPerSec: env.int('ZERO_RM_VS_SHARED_SYNC_TARGET_TPS', 1_000),
      syncWorkerCount: env.int('ZERO_RM_VS_SYNC_WORKERS', 8),
      workload: {kind: 'insert-only'},
    },
    {
      name: 'mixed-hot-row-churn',
      rowsPerTx: 20,
      payload: medium,
      targetTxPerSec: env.int('ZERO_RM_VS_MIXED_TARGET_TPS', 4_000),
      workload: {
        kind: 'mixed-row-churn',
        insertWeight: env.int('ZERO_RM_VS_MIXED_INSERT_WEIGHT', 4),
        updateWeight: env.int('ZERO_RM_VS_MIXED_UPDATE_WEIGHT', 4),
        deleteWeight: env.int('ZERO_RM_VS_MIXED_DELETE_WEIGHT', 2),
      },
    },
    {
      name: 'large-row-burst',
      rowsPerTx: 50,
      payload: large,
      targetTxPerSec: env.int('ZERO_RM_VS_LARGE_TARGET_TPS', 80),
      workload: {kind: 'insert-only'},
    },
  ];
}

function profile(
  size: PayloadProfile['size'],
  profiles: readonly PayloadProfile[],
): PayloadProfile {
  const payload = profiles.find(profile => profile.size === size);
  if (payload === undefined) {
    throw new Error(`Missing ${size} payload profile`);
  }
  return payload;
}

function filterScenarios(scenarios: Scenario[], env: EnvReader): Scenario[] {
  const filter = env.string('ZERO_RM_VS_SCENARIO');
  if (filter === undefined || filter === '') {
    return scenarios;
  }
  const selected = scenarios.filter(scenario => scenario.name === filter);
  if (selected.length === 0) {
    throw new Error(
      `Unknown ZERO_RM_VS_SCENARIO=${filter}; choices: ${scenarios
        .map(scenario => scenario.name)
        .join(', ')}`,
    );
  }
  return selected;
}
