import {isProd} from 'shared/src/config.js';

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
