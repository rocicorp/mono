import {describe, expect, test} from '@jest/globals';
import {extractVars} from './vars.js';

describe('vars', () => {
  test('extracts REFLECT_VARS_ from env', () => {
    expect(
      extractVars({
        roomDO: {},
        authDO: {},
        ['REFLECT_AUTH_API_KEY']: 'ignore me',
        ['REFLECT_VAR_FOO']: 'bar',
        ['REFLECT_VAR_BAR']: 'boom',
      }),
    ).toEqual({
      ['FOO']: 'bar',
      ['BAR']: 'boom',
    });
  });

  test('Vars cannot be modified', () => {
    const vars = extractVars({
      roomDO: {},
      authDO: {},
      ['REFLECT_AUTH_API_KEY']: 'ignore me',
      ['REFLECT_VAR_foo']: 'bar',
      ['REFLECT_VAR_bar']: 'boom',
    });
    expect(vars).toEqual({
      foo: 'bar',
      bar: 'boom',
    });

    let err;
    try {
      (vars as unknown as {foo: string}).foo = 'not-allowed';
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(TypeError);

    expect(vars).toEqual({
      foo: 'bar',
      bar: 'boom',
    });
  });

  test('Errors on non-string REFLECT_VARS_', () => {
    let err;
    try {
      extractVars({
        roomDO: {},
        authDO: {},
        ['REFLECT_AUTH_API_KEY']: 'ignore me',
        ['REFLECT_VAR_FOO']: 'bar',
        ['REFLECT_VAR_BAR']: 2,
      });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(Error);
  });
});
