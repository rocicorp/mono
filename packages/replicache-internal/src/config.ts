const isProd = process.env.NODE_ENV === 'production';

export const skipCommitDataAsserts = isProd;

export const skipAssertJSONValue = isProd;

export const skipBTreeNodeAsserts = isProd;

// Used to disable asserts ensuring internal values are not leaked.
// Temporarily disabled due to https://github.com/rocicorp/replicache-internal/issues/194.
export const skipInternalValueAsserts = true; // isProd;

// Used to disable costly deepClone of the return values in ReadTransaction.
export const skipCloneReadTransactionReturnValue = isProd;

// Used to disable cloning of some input values (like the patch coming from the
// network).
export const skipCloneInputValues = isProd;
