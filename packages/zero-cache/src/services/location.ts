import {assert} from 'shared/src/asserts.js';
import type {ServiceRunnerEnv} from './service-runner.js';

export function getDOLocation(env: ServiceRunnerEnv) {
  const locationHint = env.DO_LOCATION_HINT;
  assertDOLocation(locationHint);
  return {locationHint};
}

const DO_LOCATION_HINTS: ReadonlySet<string> = new Set([
  'wnam',
  'enam',
  'sam',
  'weur',
  'eeur',
  'apac',
  'oc',
  'afr',
  'me',
]);

function assertDOLocation(
  val: string,
): asserts val is DurableObjectLocationHint {
  assert(
    DO_LOCATION_HINTS.has(val),
    `${val} is not a valid location hint value.  Supported values: ${[
      ...DO_LOCATION_HINTS.values(),
    ].join(',')}.`,
  );
}
