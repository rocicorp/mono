export const uuid: () => string =
  typeof crypto !== 'undefined' && typeof crypto.randomUUID !== 'undefined'
    ? uuidNative
    : uuidNoNative;

export function uuidNoNative(): string {
  const numbers = new Uint8Array(36);
  crypto.getRandomValues(numbers);
  return uuidFromNumbers(numbers);
}

export function uuidNative(): string {
  return crypto.randomUUID();
}

export const enum UUIDElements {
  Random09AF,
  Random89AB,
  Hyphen,
  Version,
}

const UUID_V4_FORMAT = [
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Hyphen,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Hyphen,
  UUIDElements.Version,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Hyphen,
  UUIDElements.Random89AB,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Hyphen,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
  UUIDElements.Random09AF,
] as const;

export function uuidFromNumbers(randomNumbers: Uint8Array): string {
  return UUID_V4_FORMAT.map((kind, i) => {
    switch (kind) {
      case UUIDElements.Random09AF:
        return (randomNumbers[i] & 0b1111).toString(16);

      case UUIDElements.Random89AB:
        return ((randomNumbers[i] & 0b11) + 8).toString(16);

      case UUIDElements.Version:
        return '4';
      case UUIDElements.Hyphen:
        return '-';
    }
  }).join('');
}
