import {expect, test} from 'vitest';

declare const process: {
  env: {
    NODE_ENV?: string;
  };
};

test('process', () => {
  expect(process.env.NODE_ENV).toBe('test');
});
