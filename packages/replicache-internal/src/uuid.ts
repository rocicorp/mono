export function uuid(): string {
  const numbers = new Uint8Array(36);
  crypto.getRandomValues(numbers);
  return uuidFromNumbers(numbers);
}

const enum UuidElements {
  Random09AF,
  Random89AB,
  Hyphen,
  Version,
}

const UUID_V4_FORMAT = [
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Hyphen,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Hyphen,
  UuidElements.Version,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Hyphen,
  UuidElements.Random89AB,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Hyphen,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
  UuidElements.Random09AF,
] as const;

export function uuidFromNumbers(random_numbers: Uint8Array): string {
  return UUID_V4_FORMAT.map((kind, i) => {
    switch (kind) {
      case UuidElements.Random09AF:
        return (random_numbers[i] & 0b1111).toString(16);

      case UuidElements.Random89AB:
        return ((random_numbers[i] & 0b11) + 8).toString(16);

      case UuidElements.Version:
        return '4';
      case UuidElements.Hyphen:
        return '-';
    }
  }).join('');
}
