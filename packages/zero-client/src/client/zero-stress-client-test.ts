// we export the Zero instance so that tsc will try to compile it
// and fail if it can't output .d.ts

import {zeroStressSchema} from './zero-stress-schema-test.ts';
import {Zero} from './zero.ts';

const zeroStress = new Zero({
  schema: zeroStressSchema,
  userID: 'anon',
  server: null,
});

export {zeroStress};
