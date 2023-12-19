import {describe, expect, test} from '@jest/globals';
import {defaultPermissions, normalizePermissions} from './app-key.js';

describe('app-key', () => {
  test('default permissions', () => {
    expect(defaultPermissions()).toEqual({
      'app:publish': false,
      'env:modify': false,
      'rooms:read': false,
      'rooms:create': false,
      'rooms:close': false,
      'rooms:delete': false,
      'connections:invalidate': false,
    });
  });

  test('normalize permissions', () => {
    expect(normalizePermissions({'app:publish': true})).toEqual({
      'app:publish': true,
      'env:modify': false,
      'rooms:read': false,
      'rooms:create': false,
      'rooms:close': false,
      'rooms:delete': false,
      'connections:invalidate': false,
    });

    expect(() => normalizePermissions({invalid: true})).toThrowError;
  });
});
