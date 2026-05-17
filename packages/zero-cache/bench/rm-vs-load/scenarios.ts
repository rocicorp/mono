import {
  loadPayloadProfiles,
  smokePayloadProfiles,
  type PayloadProfile,
} from './fixtures.ts';
import {envInt, formatBytes} from './perf-utils.ts';
import type {Scenario} from './types.ts';

export function loadScenarios(full: boolean): Scenario[] {
  return filterScenarios(full ? fullScenarios() : smokeScenarios());
}

export function describeScenarios(scenarios: readonly Scenario[]): string {
  return scenarios
    .map(s => `${s.name}=${s.rowsPerTx}x${formatBytes(s.payload.bytes)}`)
    .join(', ');
}

function smokeScenarios(): Scenario[] {
  const small = profile('small', smokePayloadProfiles);
  return [
    {
      name: 'steady-small',
      rowsPerTx: 1,
      payload: small,
      targetTxPerSec: envInt('ZERO_RM_VS_TARGET_TPS', 500),
    },
  ];
}

function fullScenarios(): Scenario[] {
  const small = profile('small', loadPayloadProfiles);
  const medium = profile('medium', loadPayloadProfiles);
  const large = profile('large', loadPayloadProfiles);
  return [
    {
      name: 'single-row-flood',
      rowsPerTx: 1,
      payload: small,
      targetTxPerSec: envInt('ZERO_RM_VS_SMALL_TARGET_TPS', 1_200),
    },
    {
      name: 'medium-batch-pressure',
      rowsPerTx: 10,
      payload: medium,
      targetTxPerSec: envInt('ZERO_RM_VS_MEDIUM_TARGET_TPS', 400),
    },
    {
      name: 'medium-wide-batch-pressure',
      rowsPerTx: 20,
      payload: medium,
      targetTxPerSec: envInt('ZERO_RM_VS_MEDIUM_WIDE_TARGET_TPS', 1_000),
    },
    {
      name: 'large-row-burst',
      rowsPerTx: 50,
      payload: large,
      targetTxPerSec: envInt('ZERO_RM_VS_LARGE_TARGET_TPS', 80),
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

function filterScenarios(scenarios: Scenario[]): Scenario[] {
  const filter = process.env.ZERO_RM_VS_SCENARIO;
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
