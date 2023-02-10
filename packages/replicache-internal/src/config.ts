const isProd = process.env.NODE_ENV === 'production';

export const skipCommitDataAsserts = isProd;

export const skipAssertJSONValue = isProd;

export const skipBTreeNodeAsserts = isProd;
