import {describe, expect, test} from 'vitest';
import {cmpVersions} from './types.js';

describe('view-syncer/schema/types', () => {
  test('version comparison', () => {
    expect(
      cmpVersions(
        {stateVersion: '02', minorVersion: 1},
        {stateVersion: '01', minorVersion: 2},
      ),
    ).toBeGreaterThan(0);

    expect(
      cmpVersions(
        {stateVersion: '01', minorVersion: 2},
        {stateVersion: '02', minorVersion: 1},
      ),
    ).toBeLessThan(0);

    expect(
      cmpVersions(
        {stateVersion: '02', minorVersion: 1},
        {stateVersion: '02', minorVersion: 2},
      ),
    ).toBeLessThan(0);

    expect(
      cmpVersions(
        {stateVersion: '02', minorVersion: 2},
        {stateVersion: '02', minorVersion: 1},
      ),
    ).toBeGreaterThan(0);

    expect(
      cmpVersions({stateVersion: '02'}, {stateVersion: '02', minorVersion: 1}),
    ).toBeLessThan(0);

    expect(
      cmpVersions({stateVersion: '02', minorVersion: 1}, {stateVersion: '02'}),
    ).toBeGreaterThan(0);

    expect(
      cmpVersions(
        {stateVersion: '02', minorVersion: 2},
        {stateVersion: '02', minorVersion: 2},
      ),
    ).toBe(0);
  });
});
