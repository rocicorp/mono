/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
import {isProd} from '../../shared/src/config.ts';

export {
  isProd as skipBTreeNodeAsserts,
  isProd as skipCommitDataAsserts,
  /**
   * In debug mode we deeply freeze the values we read out of the IDB store and we
   * deeply freeze the values we put into the stores.
   */
  isProd as skipFreeze,
  /**
   * In debug mode we assert that chunks and BTree data is deeply frozen. In
   * release mode we skip these asserts.
   */
  isProd as skipFrozenAsserts,
  isProd as skipGCAsserts,
};
