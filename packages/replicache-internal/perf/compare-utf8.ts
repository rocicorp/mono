import {compareUTF8} from 'compare-utf8';
import {makeRandomASCIIStrings, makeRandomStrings} from './data.js';
import type {Benchmark} from './perf.js';

const encoder = new TextEncoder();

function stringToUint8Array(s: string): Uint8Array {
  return encoder.encode(s);
}

export function benchmarks(): Array<Benchmark> {
  return [
    perf(stringCompare, 'String compare', makeRandomStrings),
    perf(collateCompare, 'Intl.Collator', makeRandomStrings),
    perf(compareUTF8, 'Compare UTF8', makeRandomStrings),
    perf(encoderCompare, 'TextEncoder', makeRandomStrings),
    perf(localeCompare, 'String.localeCompare', makeRandomStrings),

    perf(stringCompare, 'String compare ASCII', makeRandomASCIIStrings),
    perf(collateCompare, 'Intl.Collator ASCII', makeRandomASCIIStrings),
    perf(compareUTF8, 'Compare UTF8 ASCII', makeRandomASCIIStrings),
    perf(encoderCompare, 'TextEncoder ASCII', makeRandomASCIIStrings),
    perf(localeCompare, 'String.localeCompare ASCII', makeRandomASCIIStrings),
  ];
}

const NUM_STRINGS = 50_000;
const STRING_LENGTH = 50;

function stringCompare(a: string, b: string): number {
  return a === b ? 0 : a < b ? -1 : 1;
}

const collator = new Intl.Collator('en');
function collateCompare(a: string, b: string): number {
  return collator.compare(a, b);
}

function encoderCompare(a: string, b: string): number {
  const aUint8 = stringToUint8Array(a);
  const bUint8 = stringToUint8Array(b);
  return compareUint8Arrays(aUint8, bUint8);
}

function localeCompare(a: string, b: string): number {
  return a.localeCompare(b);
}

function perf(
  compare: (a: string, b: string) => number,
  name: string,
  makeRandomStrings: (numStrings: number, strLen: number) => string[],
): Benchmark {
  let randomStrings: string[];
  const results = [];
  return {
    name,
    group: 'compare-utf8',
    setup() {
      randomStrings = makeRandomStrings(NUM_STRINGS, STRING_LENGTH);
    },
    run() {
      for (let i = 0; i < randomStrings.length - 1; i++) {
        results.push(compare(randomStrings[i], randomStrings[i + 1]));
      }
    },
  };
}
function compareUint8Arrays(a: Uint8Array, b: Uint8Array): number {
  const aLength = a.length;
  const bLength = b.length;
  const length = Math.min(aLength, bLength);
  for (let i = 0; i < length; i++) {
    if (a[i] !== b[i]) {
      return a[i] - b[i];
    }
  }
  return aLength - bLength;
}
