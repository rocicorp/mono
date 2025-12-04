// we export the Zero instance so that tsc will try to compile it
// and fail if it can't output .d.ts

import {mutators} from './zero-stress-mutators-test.ts';
import {zeroStressSchema} from './zero-stress-schema-test.ts';
import {Zero} from './zero.ts';

const zeroStress = new Zero({
  schema: zeroStressSchema,
  userID: 'anon',
  cacheURL: null,
  mutators,
});

export {zeroStress};
