import {compareUTF8} from 'compare-utf8';
import {
  makeRandomASCIIStrings,
  makeRandomStrings,
} from '../../shared/src/test-data.ts';
import {bench, describe} from '../../shared/src/bench.ts';

const NUM_STRINGS = 50_000;
const STRING_LENGTH = 50;

const encoder = new TextEncoder();

function stringToUint8Array(s: string): Uint8Array {
  return encoder.encode(s);
}

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

function makeCompareBench(
  name: string,
  compare: (a: string, b: string) => number,
  makeStrings: (numStrings: number, strLen: number) => string[],
) {
  const randomStrings = makeStrings(NUM_STRINGS, STRING_LENGTH);
  const results: number[] = [];
  bench(name, () => {
    for (let i = 0; i < randomStrings.length - 1; i++) {
      results.push(compare(randomStrings[i], randomStrings[i + 1]));
    }
  });
}

describe('compare-utf8', () => {
  makeCompareBench('String compare', stringCompare, makeRandomStrings);
  makeCompareBench('Intl.Collator', collateCompare, makeRandomStrings);
  makeCompareBench('Compare UTF8', compareUTF8, makeRandomStrings);
  makeCompareBench('TextEncoder', encoderCompare, makeRandomStrings);
  makeCompareBench('String.localeCompare', localeCompare, makeRandomStrings);

  makeCompareBench('String compare ASCII', stringCompare, makeRandomASCIIStrings);
  makeCompareBench('Intl.Collator ASCII', collateCompare, makeRandomASCIIStrings);
  makeCompareBench('Compare UTF8 ASCII', compareUTF8, makeRandomASCIIStrings);
  makeCompareBench('TextEncoder ASCII', encoderCompare, makeRandomASCIIStrings);
  makeCompareBench(
    'String.localeCompare ASCII',
    localeCompare,
    makeRandomASCIIStrings,
  );
});
