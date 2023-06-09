import {expect} from 'chai';

declare const process: {
  env: {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    NODE_ENV?: string;
  };
};

test('process', () => {
  expect(process.env.NODE_ENV).to.equal('development');
});
