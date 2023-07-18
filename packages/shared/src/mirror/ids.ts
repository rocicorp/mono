import {objects, predicates} from 'friendly-words';
import {webcrypto as crypto} from 'node:crypto';
import * as base32 from '../base32.js';
import * as base62 from '../base62.js';

const tempUint64Array = new BigUint64Array(1);

function newRandomUint64(): bigint {
  // TODO(arv): For some very odd reason Firebase functions does not have
  // crypto.getRandomValues. What version of node is it running?

  // TODO(arv): Do we need this in a browser? Then we need to make it async and
  // conditionally load the node module using dynamic import.
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
  if (pred1 === pred2) {
    return newAppScriptName(appID);
  }
  const obj = randomSample(objects);

  const appIDNum = base62.decode(appID);
  const appIDBase32 = base32.encode(appIDNum);

  return `${pred1}-${pred2}-${obj}-${appIDBase32}`;
}

function randomSample<T>(arr: T[]): T {
  return arr[(Math.random() * arr.length) | 0];
}

export function newDeploymentID(): string {
  return Date.now() + '';
}
