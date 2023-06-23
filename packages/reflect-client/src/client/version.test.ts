import {expect} from 'chai';
import {version as versionShared} from 'reflect-shared';
import {version} from './version.js';

test('version', () => {
  expect(typeof version).equal('string');
  expect(version).equal(versionShared);
});
