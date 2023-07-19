import {objects, predicates} from 'friendly-words';
import * as base62 from '../base62.js';

const tempUint64Array = new BigUint64Array(1);

function newRandomUint64(): bigint {
  crypto.getRandomValues(tempUint64Array);
  return tempUint64Array[0];
}

export function newTeamID(): string {
  const n = newRandomUint64();
  return base62.encode(n);
}

export function newAppIDAsNumber(): number {
  return Date.now();
}

export function newAppID(n = newAppIDAsNumber()): string {
  return base62.encode(BigInt(n));
}

/**
 * The app script name is used for the name of the cloudflare worker script name
 * which does not allow uppercase letters. We therefore use base36 encoding of
 * the numeric app ID.
 */
export function newAppScriptName(appIDNumber: number): string {
  const pred1 = randomSample(predicates);
  const pred2 = randomSample(predicates);
  if (pred1 === pred2) {
    return newAppScriptName(appIDNumber);
  }
  const obj = randomSample(objects);
  const appIDBase36 = appIDNumber.toString(36);
  return `${pred1}-${pred2}-${obj}-${appIDBase36}`;
}

function randomSample<T>(arr: T[]): T {
  return arr[(Math.random() * arr.length) | 0];
}

export function newDeploymentID(): string {
  return Date.now() + '';
}
