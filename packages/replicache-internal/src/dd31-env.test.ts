import {expect} from '@esm-bundle/chai';

test('dd31 env', async () => {
  expect(DD31).is.oneOf([true, false]);
});
