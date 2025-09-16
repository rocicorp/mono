/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/await-thenable, @typescript-eslint/no-misused-promises, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-floating-promises, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/require-await, @typescript-eslint/no-empty-object-type, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error */
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
  );
});
