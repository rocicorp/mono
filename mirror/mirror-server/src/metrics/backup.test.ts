import type {Bucket} from '@google-cloud/storage';
import {afterEach, describe, expect, jest, test} from '@jest/globals';
import {Analytics} from 'cloudflare-api/src/analytics.js';
import {FetchMocker} from 'shared/src/fetch-mocker.js';
import {backupWeekBefore, checkResults} from './backup.js';

const WELL_FORMED_RESULT =
  `{"_sample_interval":1,"blob1":"CzCv9TyiF7u","blob10":"","blob11":"","blob12":"","blob13":"","blob14":"","blob15":"","blob16":"","blob17":"","blob18":"","blob19":"","blob2":"loop2al0","blob20":"","blob3":"","blob4":"","blob5":"","blob6":"","blob7":"","blob8":"","blob9":"","dataset":"RunningConnectionSeconds","double1":60.001,"double10":0,"double11":0,"double12":0,"double13":0,"double14":0,"double15":0,"double16":0,"double17":0,"double18":0,"double19":0,"double2":60.001,"double20":0,"double3":0,"double4":0,"double5":0,"double6":0,"double7":0,"double8":0,"double9":0,"index1":"","timestamp":"2023-11-08 19:52:46"}\n` +
  `{"_sample_interval":1,"blob1":"CzCv9TyiF7u","blob10":"","blob11":"","blob12":"","blob13":"","blob14":"","blob15":"","blob16":"","blob17":"","blob18":"","blob19":"","blob2":"loop2al0","blob20":"","blob3":"","blob4":"","blob5":"","blob6":"","blob7":"","blob8":"","blob9":"","dataset":"RunningConnectionSeconds","double1":60.001,"double10":0,"double11":0,"double12":0,"double13":0,"double14":0,"double15":0,"double16":0,"double17":0,"double18":0,"double19":0,"double2":60.001,"double20":0,"double3":0,"double4":0,"double5":0,"double6":0,"double7":0,"double8":0,"double9":0,"index1":"","timestamp":"2023-11-08 19:53:46"}\n`;

describe('metrics-backup', () => {
  test('checkResults', () => {
    expect(checkResults(WELL_FORMED_RESULT)).toBe(2);
  });

  test('checkResults (malformed)', () => {
    expect(() => checkResults(`Couldn't parse the query blah blah blah`))
      .toThrowError;
  });

  describe('backupWeekBefore', () => {
    const file = {save: jest.fn()};
    const bucket = {file: jest.fn().mockImplementation(() => file)};

    afterEach(() => {
      jest.clearAllMocks();
    });

    function textResult(text: unknown) {
      return {
        ok: true,
        status: 200,
        text: () => Promise.resolve(text),
      } as unknown as Response;
    }

    test('success', async () => {
      const fetcher = new FetchMocker(textResult).result(
        'POST',
        '/analytics',
        WELL_FORMED_RESULT,
      );

      await backupWeekBefore(
        new Date(Date.UTC(2023, 5, 13)), // Tuesday, June 13
        new Analytics({apiToken: 'api-token', accountID: 'my-account-id'}),
        'RunningConnectionSeconds',
        bucket as unknown as Bucket,
      );

      expect(fetcher.bodys()).toEqual([
        'SELECT * FROM RunningConnectionSeconds WHERE timestamp >= toDateTime(1685836800) AND timestamp < toDateTime(1685923200) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM RunningConnectionSeconds WHERE timestamp >= toDateTime(1685923200) AND timestamp < toDateTime(1686009600) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM RunningConnectionSeconds WHERE timestamp >= toDateTime(1686009600) AND timestamp < toDateTime(1686096000) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM RunningConnectionSeconds WHERE timestamp >= toDateTime(1686096000) AND timestamp < toDateTime(1686182400) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM RunningConnectionSeconds WHERE timestamp >= toDateTime(1686182400) AND timestamp < toDateTime(1686268800) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM RunningConnectionSeconds WHERE timestamp >= toDateTime(1686268800) AND timestamp < toDateTime(1686355200) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM RunningConnectionSeconds WHERE timestamp >= toDateTime(1686355200) AND timestamp < toDateTime(1686441600) ORDER BY timestamp FORMAT JSONEachRow',
      ]);

      expect(bucket.file).toHaveBeenCalledWith(
        'my-account-id/RunningConnectionSeconds/2023-06-04~2023-06-11', // Sunday, June 4 - 11
      );
      expect(file.save).toHaveBeenCalledWith(WELL_FORMED_RESULT.repeat(7), {
        resumable: false,
        gzip: true,
        contentType: 'text/plain',
      });
    });

    test('empty', async () => {
      const fetcher = new FetchMocker(textResult).result(
        'POST',
        '/analytics',
        '',
      );

      await backupWeekBefore(
        new Date(Date.UTC(2023, 5, 11, 6)), // Sunday, June 11, 6:00AM
        new Analytics({apiToken: 'api-token', accountID: 'tuesday-account-id'}),
        'ConnectionLifetimes',
        bucket as unknown as Bucket,
      );

      expect(fetcher.bodys()).toEqual([
        'SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1685836800) AND timestamp < toDateTime(1685923200) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1685923200) AND timestamp < toDateTime(1686009600) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1686009600) AND timestamp < toDateTime(1686096000) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1686096000) AND timestamp < toDateTime(1686182400) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1686182400) AND timestamp < toDateTime(1686268800) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1686268800) AND timestamp < toDateTime(1686355200) ORDER BY timestamp FORMAT JSONEachRow',
        'SELECT * FROM ConnectionLifetimes WHERE timestamp >= toDateTime(1686355200) AND timestamp < toDateTime(1686441600) ORDER BY timestamp FORMAT JSONEachRow',
      ]);

      expect(bucket.file).toHaveBeenCalledWith(
        'tuesday-account-id/ConnectionLifetimes/2023-06-04~2023-06-11', // Sunday, June 4 - 11
      );
      expect(file.save).toHaveBeenCalledWith('', {
        resumable: false,
        gzip: true,
        contentType: 'text/plain',
      });
    });

    test('too close to Sunday', () => {
      expect(() =>
        backupWeekBefore(
          new Date(Date.UTC(2023, 5, 11, 5)), // Sunday, June 11, 5:00AM
          new Analytics({apiToken: 'api-token', accountID: 'my-account-id'}),
          'RunningConnectionSeconds',
          bucket as unknown as Bucket,
        ),
      ).toThrowError;

      expect(bucket.file).not.toBeCalled;
    });

    test('malformed response', () => {
      new FetchMocker(textResult).result(
        'POST',
        '/analytics',
        'Some error message',
      );

      expect(() =>
        backupWeekBefore(
          new Date(Date.UTC(2023, 5, 11, 6)), // Sunday, June 11, 6:00AM
          new Analytics({apiToken: 'api-token', accountID: 'my-account-id'}),
          'RunningConnectionSeconds',
          bucket as unknown as Bucket,
        ),
      ).toThrowError;

      expect(bucket.file).not.toBeCalled;
    });
  });
});
