import {expect} from '@esm-bundle/chai';

test('process', () => {
  expect(process.env.NODE_ENV).to.equal('development');
});
