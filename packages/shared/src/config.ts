declare const process: {
  env: {
    NODE_ENV?: string;
  };
};

export const isProd = process.env.NODE_ENV === 'production';

export {isProd as skipAssertJSONValue};
