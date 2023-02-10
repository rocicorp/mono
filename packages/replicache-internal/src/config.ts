const isProd = process.env.NODE_ENV === 'production';

export const skipCommitDataAsserts = isProd;

export const skipAssertJSONValue = isProd;

export const skipBTreeNodeAsserts = isProd;

/**
 * In debug mode we assert that chunks and BTree data is deeply frozen. In
 * release mode we skip these asserts.
 */
export const skipFrozenAsserts = isProd;

/**
 * In debug mode we deeply freeze the values we read out of the IDB store and we
 * deeply freeze the values we put into the stores.
 */
export const skipFreeze = isProd;
