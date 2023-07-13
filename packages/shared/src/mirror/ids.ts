import {objects, predicates} from 'friendly-words';
import * as base62 from 'shared/src/base62.js';

const tempUint64Array = new BigUint64Array(1);

function newRandomUint64(): bigint {
  crypto.getRandomValues(tempUint64Array);
  return tempUint64Array[0];
}

export function newTeamID(): string {
  const n = newRandomUint64();
  return base62.encode(n);
}

export function newAppID(): string {
  tempUint64Array[0] = BigInt(Date.now());
  return base62.encode(tempUint64Array[0]);
}

export function newAppScriptName(appID: string): string {
  const pred1 = randomSample(predicates);
  const pred2 = randomSample(predicates);
  const obj = randomSample(objects);
  if (pred1 === pred2) {
    return newAppScriptName(appID);
  }
  return `${pred1}-${pred2}-${obj}-${appID}`;
}

function randomSample<T>(arr: T[]): T {
  return arr[(Math.random() * arr.length) | 0];
}

export function newDeploymentID(): string {
  return Date.now() + '';
}
