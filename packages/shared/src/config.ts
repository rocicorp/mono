declare const process: {
  env: {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    NODE_ENV?: string;
  };
};

export const isProd = process.env.NODE_ENV === 'production';

export {isProd as skipAssertJSONValue};
