import {expect} from 'chai';
import * as sinon from 'sinon';
import {uuid, uuidFromNumbers, uuidNoNative} from './uuid.js';

teardown(() => {
  sinon.restore();
});

const re = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$/;

test('uuid', () => {
  const arr = new Uint8Array(36);

  expect(uuidFromNumbers(arr)).to.equal('00000000-0000-4000-8000-000000000000');

  arr.fill(1);
  expect(uuidFromNumbers(arr)).to.equal('11111111-1111-4111-9111-111111111111');

  arr.fill(15);
  expect(uuidFromNumbers(arr)).to.equal('ffffffff-ffff-4fff-bfff-ffffffffffff');

  expect(re.test(uuidFromNumbers(arr))).to.be.true;

  expect(re.test(uuid())).to.be.true;
});

test('uuidNoNative', () => {
  expect(re.test(uuidNoNative())).to.be.true;

  let i = 0;
  sinon.stub(Math, 'random').callsFake(() => i++ / 256);

  expect(uuidNoNative()).equal('01234567-9abc-4f01-b456-89abcdef0123');
});
