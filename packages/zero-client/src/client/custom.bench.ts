import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {bench} from 'vitest';
import {zeroData} from '../../../replicache/src/transactions.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {generateSchema} from '../../../zql/src/query/test/schema-gen.ts';
import {TransactionImpl} from './custom.ts';
import type {WriteTransaction} from './replicache-types.ts';

const rng = generateMersenne53Randomizer(400);
const schema = generateSchema(
  () => rng.next(),
  new Faker({
    locale: en,
    randomizer: rng,
  }),
  200,
);

bench('big schema', () => {
  new TransactionImpl(
    createSilentLogContext(),
    {
      [zeroData]: {},
    } as unknown as WriteTransaction,
    schema,
    'bench',
  );
});
