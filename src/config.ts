const isReleaseBuild = process.env.NODE_ENV === 'production';

export {
  isReleaseBuild,
  isReleaseBuild as skipCommitDataAsserts,
  isReleaseBuild as skipAssertJSONValue,
  isReleaseBuild as skipBTreeNodeAsserts,
  isReleaseBuild as skipGCAsserts,

  /**
   * In debug mode we assert that chunks and BTree data is deeply frozen. In
   * release mode we skip these asserts.
   */
  isReleaseBuild as skipFrozenAsserts,

  /**
   * In debug mode we deeply freeze the values we read out of the IDB store and we
   * deeply freeze the values we put into the stores.
   */
  isReleaseBuild as skipFreeze,
};
