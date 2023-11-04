import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  test,
} from '@jest/globals';
import {MAX_SERVER_VARIABLES} from 'mirror-schema/src/external/vars.js';
import {existsSync} from 'node:fs';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import {UserError} from '../error.js';
import {
  deleteDevVars,
  listDevVars,
  setDevVars,
  setFileOverriddeForTests,
} from './vars.js';

describe('dev vars', () => {
  let varsFile: string;

  beforeAll(async () => {
    const dir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'reflect-dev-vars-test-'),
    );
    varsFile = path.join(dir, 'dev-vars.env');
    setFileOverriddeForTests(varsFile);
  });

  afterEach(async () => {
    if (existsSync(varsFile)) {
      await fs.unlink(varsFile);
    }
  });

  afterAll(() => {
    setFileOverriddeForTests(undefined);
  });

  test('empty list', () => {
    expect(listDevVars()).toEqual({});
  });

  /* eslint-disable @typescript-eslint/naming-convention */
  test('set and list', async () => {
    setDevVars({
      FOO: 'bar',
      BAR: 'can have spaces',
    });
    expect(await fs.readFile(varsFile, 'utf-8')).toBe(
      'BAR=can have spaces\nFOO=bar',
    );
    expect(listDevVars()).toEqual({
      BAR: 'can have spaces',
      FOO: 'bar',
    });
  });

  test('set keys with javascript Object method names', async () => {
    setDevVars({
      FOO: 'bar',
      BAR: 'can have spaces',
      toString: 'this should still work',
    });
    expect(await fs.readFile(varsFile, 'utf-8')).toBe(
      'BAR=can have spaces\nFOO=bar\ntoString=this should still work',
    );
    expect(listDevVars()).toEqual({
      BAR: 'can have spaces',
      FOO: 'bar',
      toString: 'this should still work',
    });
  });

  test('set values with newlines and equals', async () => {
    setDevVars({
      FOO: 'bar',
      BAR: 'can\nhave\nnewlines',
      BAZ: 'can\nhave\nnewlines=with\nequals=sign',
    });
    expect(await fs.readFile(varsFile, 'utf-8')).toBe(
      'BAR=can\\nhave\\nnewlines\n' +
        'BAZ=can\\nhave\\nnewlines\\=with\\nequals\\=sign\n' +
        'FOO=bar',
    );
    expect(listDevVars()).toEqual({
      BAR: 'can\nhave\nnewlines',
      BAZ: 'can\nhave\nnewlines=with\nequals=sign',
      FOO: 'bar',
    });
  });

  test('set and delete list', async () => {
    setDevVars({
      FOO: 'bar',
      BAR: 'can have spaces',
      BAZ: 'buzz',
    });

    deleteDevVars(['FOO', 'BOO']);

    expect(await fs.readFile(varsFile, 'utf-8')).toBe(
      'BAR=can have spaces\nBAZ=buzz',
    );
    expect(listDevVars()).toEqual({
      BAR: 'can have spaces',
      BAZ: 'buzz',
    });
  });

  test('invalid names', async () => {
    setDevVars({
      FOO: 'bar',
      BAR: 'can have spaces',
      BAZ: 'buzz',
    });

    let err;
    try {
      setDevVars({
        FOO: 'boo',
        ['keys cannot have spaces']: 'should fail',
      });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(UserError);

    // File should be unchanged
    expect(await fs.readFile(varsFile, 'utf-8')).toBe(
      'BAR=can have spaces\nBAZ=buzz\nFOO=bar',
    );
    expect(listDevVars()).toEqual({
      FOO: 'bar',
      BAR: 'can have spaces',
      BAZ: 'buzz',
    });
  });

  test('variable name size limit', () => {
    setDevVars({
      ['a'.repeat(1024)]: 'is not too big',
    });

    let err;
    try {
      setDevVars({
        ['b'.repeat(1025)]: 'is too big',
      });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(UserError);

    expect(listDevVars()).toEqual({
      ['a'.repeat(1024)]: 'is not too big',
    });
  });

  test('variable size limit', () => {
    setDevVars({
      NOT_TOO_BIG: 'a'.repeat(5100),
    });

    let err;
    try {
      setDevVars({
        TOO_BIG: 'a'.repeat(5121),
      });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(UserError);

    expect(listDevVars()).toEqual({
      NOT_TOO_BIG: 'a'.repeat(5100),
    });
  });

  test('max vars', async () => {
    setDevVars(
      Object.fromEntries(
        Array.from({length: MAX_SERVER_VARIABLES - 1}, (_, i) => [
          `${i}`,
          `${i}_val`,
        ]),
      ),
    );

    let err;
    try {
      setDevVars({
        TWO: 'more',
        SHOULD: 'fail',
      });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(UserError);

    setDevVars({
      ONE: 'should succeed',
    });

    expect(await fs.readFile(varsFile, 'utf-8')).toBe(
      '0=0_val\n1=1_val\n2=2_val\n3=3_val\n4=4_val\n5=5_val\n6=6_val\n7=7_val\n8=8_val\n9=9_val\n10=10_val\n' +
        '11=11_val\n12=12_val\n13=13_val\n14=14_val\n15=15_val\n16=16_val\n17=17_val\n18=18_val\n19=19_val\n20=20_val\n' +
        '21=21_val\n22=22_val\n23=23_val\n24=24_val\n25=25_val\n26=26_val\n27=27_val\n28=28_val\n29=29_val\n30=30_val\n' +
        '31=31_val\n32=32_val\n33=33_val\n34=34_val\n35=35_val\n36=36_val\n37=37_val\n38=38_val\n39=39_val\n40=40_val\n' +
        '41=41_val\n42=42_val\n43=43_val\n44=44_val\n45=45_val\n46=46_val\n47=47_val\n48=48_val\n' +
        'ONE=should succeed',
    );
  });
  /* eslint-enable @typescript-eslint/naming-convention */
});
