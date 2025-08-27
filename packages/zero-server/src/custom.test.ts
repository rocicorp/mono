import {expectTypeOf, test} from 'vitest';
import type {CustomMutatorDefs} from './custom.ts';
import type {CustomMutatorDefs as CustomMutatorClientDefs} from '../../zero-client/src/client/custom.ts';

test('server mutator type is compatible with client mutator type', () => {
  expectTypeOf<
    CustomMutatorDefs<unknown>
  >().toExtend<CustomMutatorClientDefs>();
});
