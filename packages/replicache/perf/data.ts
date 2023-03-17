import {getSizeOfValue} from '../src/size-of-value.js';

export type RandomDataType = 'string' | 'object' | 'arraybuffer' | 'blob';
export type RandomDatum = string | Record<string, string> | ArrayBuffer | Blob;
export type RandomData = RandomDatum[];

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

export function randomObject(len: number): Record<string, string> {
  const ret: Record<string, string> = {};
  for (let i = 0; i < Math.min(16, len); i++) {
    ret[`k${i}`] = randomString(Math.ceil(len / 16));
  }
  return ret;
}

export function makeRandomStrings(
  numStrings: number,
  strLen: number,
): string[] {
  return Array.from({length: numStrings}, () => randomString(strLen));
}

export function makeRandomASCIIStrings(
  numStrings: number,
  strLen: number,
): string[] {
  return Array.from({length: numStrings}, () => randomASCIIString(strLen));
}

export function randomString(len: number): string {
  return randomStringInternal(len, 256);
}

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

function randomBlob(len: number): Blob {
  return new Blob([randomUint8Array(len)]);
}

function randomUint8Array(len: number): Uint8Array {
  const arr = new Uint8Array(len);
  for (let i = 0; i < len; i++) {
    arr[i] = (Math.random() * 256) | 0;
  }
  return arr;
}

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

export function jsonObjectTestData(targetSize: number): TestDataObject {
  const stringSize = targetSize - baseSize;
  return {
    boolean: randomBoolean(),
    float64: randomFloat64(),
    int32: randomInt32(),
    null: null,
    ascii: randomASCIIString(stringSize / 2),
    nonAscii: randomString(stringSize / 2),
  };
}

function randomBoolean() {
  return Math.random() < 0.5;
}

function randomFloat64() {
  let x = Math.random();
  while (x === (x | 0)) {
    x = Math.random();
  }
  return x;
}

function randomInt32() {
  return (Math.random() * 2 ** 31 - 1) | 0;
}

type TmcwData = {
  type: string;
  features: {
    type: string;
    geometry: {
      type: string;
      coordinates: [number, number][];
    };
    properties: {
      /* eslint-disable @typescript-eslint/naming-convention */
      rwdb_rr_id: number;
      mult_track: number;
      electric: number;
      other_code: number;
      category: number;
      disp_scale: string;
      add: number;
      featurecla: string;
      scalerank: number;
      natlscale: number;
      part: string;
      continent: string;
      /* eslint-enable @typescript-eslint/naming-convention */
    };
  }[];
};

export async function getTmcwData(): Promise<TmcwData> {
  const response = await fetch(
    new URL('./resources/tmcw.json', import.meta.url),
  );
  return response.json();
}
