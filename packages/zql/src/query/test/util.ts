import type {Faker} from '@faker-js/faker';
import type {ValueType} from '../../../../zero-types/src/schema-value.ts';

export type Rng = () => number;

export function selectRandom<T>(rng: Rng, values: readonly T[]): T {
  return values[Math.floor(rng() * values.length)];
}

export function generateUniqueValues<T>(
  generator: () => T,
  length: number,
): T[] {
  const values = new Set<T>();
  while (values.size < length) {
    values.add(generator());
  }
  return Array.from(values);
}

export function shuffle<T>(rng: Rng, array: T[]): T[] {
  const shuffled = [...array];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  return shuffled;
}

export function randomValueForType(
  rng: Rng,
  faker: Faker,
  type: ValueType,
  optional: boolean | undefined,
): string | boolean | number | null | Record<string, unknown> {
  if (optional && rng() < 0.1) {
    return null;
  }
  switch (type) {
    case 'string':
      return faker.lorem.words();
    case 'boolean':
      return faker.datatype.boolean();
    case 'number':
      return faker.datatype.boolean()
        ? faker.number.int()
        : faker.number.float();
    case 'json':
      // TODO: generate random JSON
      return {};
    case 'null':
      return null;
  }
}
