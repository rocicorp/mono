import {expect} from '@esm-bundle/chai';
import {uuid, uuidFromNumbers} from './uuid';

test('uuid', () => {
  const arr = new Uint8Array(36);

  expect(uuidFromNumbers(arr)).to.equal('00000000-0000-4000-8000-000000000000');

  arr.fill(1);
  expect(uuidFromNumbers(arr)).to.equal('11111111-1111-4111-9111-111111111111');

  arr.fill(15);
  expect(uuidFromNumbers(arr)).to.equal('ffffffff-ffff-4fff-bfff-ffffffffffff');

  const re =
    /^[0-9:A-z]{8}-[0-9:A-z]{4}-4[0-9:A-z]{3}-[0-9:A-z]{4}-[0-9:A-z]{12}$/;

  expect(re.test(uuidFromNumbers(arr))).to.be.true;

  expect(re.test(uuid())).to.be.true;
});
