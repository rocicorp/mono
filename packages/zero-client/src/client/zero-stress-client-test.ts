// we export the Zero instance so that tsc will try to compile it
// and fail if it can't output .d.ts

import {zeroStressSchema} from './zero-stress-schema-test.ts';
import {Zero} from './zero.ts';

const zeroStress = new Zero({
  schema: zeroStressSchema,
  userID: 'anon',
  cacheURL: null,
  // TODO(0xcadams): we need to add mutators back when we have a solution
  // for simplifying the Zero type params (e.g. we remove MD)
  // mutators,
});

export {zeroStress};
