/**
 * Shared utilities for generating random test data.
 * Used by both replicache-perf and benchmark tests.
 */

import {getSizeOfValue} from './size-of-value.ts';

export type RandomDataType = 'string' | 'object' | 'arraybuffer' | 'blob';
export type RandomDatum = string | Record<string, string> | ArrayBuffer | Blob;
export type RandomData = RandomDatum[];

/**
 * Generate an array of random data of the specified type and size.
 */
export function randomData(
  type: RandomDataType,
  len: number,
  datumSize: number,
): RandomData {
  return Array.from({length: len}).map(() => randomDatum(type, datumSize));
}

function randomDatum(type: RandomDataType, len: number): RandomDatum {
  switch (type) {
    case 'string':
      return randomString(len);
    case 'object':
      return randomObject(len);
    case 'arraybuffer':
      return randomUint8Array(len).buffer;
    case 'blob':
      return randomBlob(len);
    default:
      throw new Error('unsupported');
  }
}

/**
 * Generate a random string of the specified length.
 * Characters are in the range 1-255 (no null characters).
 */
export function randomString(len: number): string {
  return randomStringInternal(len, 256);
}

/**
 * Generate a random ASCII string of the specified length.
 * Characters are in the range 1-127 (no null characters).
 */
export function randomASCIIString(len: number): string {
  return randomStringInternal(len, 128);
}

// Shared array buffer to reduce allocations.
const codes: number[] = [];
function randomStringInternal(len: number, max: number): string {
  for (let i = 0; i < len; i++) {
    // We use random strings for index map keys and those cannot contain \0.
    codes[i] = ((Math.random() * (max - 1)) | 0) + 1;
  }
  codes.length = len;
  return String.fromCharCode(...codes);
}

/**
 * Generate a random object with string keys and values.
 * The object will have up to 16 properties.
 */
export function randomObject(len: number): Record<string, string> {
  const ret: Record<string, string> = {};
  for (let i = 0; i < Math.min(16, len); i++) {
    ret[`k${i}`] = randomString(Math.ceil(len / 16));
  }
  return ret;
}

/**
 * Generate an array of random strings.
 */
export function makeRandomStrings(
  numStrings: number,
  strLen: number,
): string[] {
  return Array.from({length: numStrings}, () => randomString(strLen));
}

/**
 * Generate an array of random ASCII strings.
 */
export function makeRandomASCIIStrings(
  numStrings: number,
  strLen: number,
): string[] {
  return Array.from({length: numStrings}, () => randomASCIIString(strLen));
}

function randomBlob(len: number): Blob {
  return new Blob([randomUint8Array(len)]);
}

function randomUint8Array(len: number): Uint8Array<ArrayBuffer> {
  const arr = new Uint8Array(len);
  for (let i = 0; i < len; i++) {
    arr[i] = (Math.random() * 256) | 0;
  }
  return arr;
}

/**
 * Generate a random boolean value.
 */
export function randomBoolean(): boolean {
  return Math.random() < 0.5;
}

/**
 * Generate a random float64 that is not an integer.
 */
export function randomFloat64(): number {
  let x = Math.random();
  while (x === (x | 0)) {
    x = Math.random();
  }
  return x;
}

/**
 * Generate a random 32-bit signed integer.
 */
export function randomInt32(): number {
  return (Math.random() * 2 ** 31 - 1) | 0;
}

/**
 * Test data object structure used in benchmarks and performance tests.
 */
export type TestDataObject = {
  boolean: boolean;
  float64: number;
  int32: number;
  null: null;
  ascii: string;
  nonAscii: string;
};

const emptyTestData: TestDataObject = {
  boolean: false,
  float64: Math.PI,
  int32: 0,
  null: null,
  ascii: '',
  nonAscii: '',
};

const baseSize = getSizeOfValue(emptyTestData);

/**
 * Generate an array of test data objects.
 */
export function jsonArrayTestData(
  numKeys: number,
  targetSize: number,
): TestDataObject[] {
  const arr: TestDataObject[] = [];
  for (let i = 0; i < numKeys; i++) {
    arr.push(jsonObjectTestData(targetSize));
  }
  return arr;
}

/**
 * Generate a single test data object with the specified target size.
 */
export function jsonObjectTestData(targetSize: number): TestDataObject {
  const stringSize = Math.max(0, targetSize - baseSize);
  return {
    boolean: randomBoolean(),
    float64: randomFloat64(),
    int32: randomInt32(),
    null: null,
    ascii: randomASCIIString(stringSize / 2),
    nonAscii: randomString(stringSize / 2),
  };
}
