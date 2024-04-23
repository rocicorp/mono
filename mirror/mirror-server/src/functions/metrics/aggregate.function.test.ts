import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import {getFirestore} from 'firebase-admin/firestore';
import {AGGREGATIONS, aggregationPath} from 'mirror-schema/src/metrics.js';
import {
  DEFAULT_PROVIDER_ID,
  providerDataConverter,
  providerPath,
} from 'mirror-schema/src/provider.js';
import {FetchMocker} from 'shared/src/fetch-mocker.js';
import {TestSecrets} from '../../secrets/test-utils.js';
import {aggregate} from './aggregate.function.js';

describe('metrics-aggregate', () => {
  initializeApp({projectId: 'metrics-aggregate-function-test'});
  const firestore = getFirestore();
  const CLOUDFLARE_ACCOUNT_ID = 'foo-cloudflare-account';

  function testSecrets() {
    return new TestSecrets([
      `${DEFAULT_PROVIDER_ID}_api_token`,
      'latest',
      'api-token',
    ]);
  }

  beforeAll(async () => {
    await firestore
      .doc(providerPath(DEFAULT_PROVIDER_ID))
      .withConverter(providerDataConverter)
      .create({
        accountID: CLOUDFLARE_ACCOUNT_ID,
        defaultMaxApps: 3,
        defaultZone: {
          zoneID: 'zone-id',
          zoneName: 'reflect-o-rama.net',
        },
        dispatchNamespace: 'foo',
      });
  });

  afterEach(async () => {
    jest.clearAllMocks();
    const aggregations = await firestore
      .collection(AGGREGATIONS)
      .listDocuments();
    if (aggregations.length) {
      const batch = firestore.batch();
      aggregations.forEach(doc => batch.delete(doc));
      await batch.commit();
    }
  });

  afterAll(async () => {
    await firestore.doc(providerPath(DEFAULT_PROVIDER_ID)).delete();
  });

  async function runAggregate(scheduleTimeMs: number): Promise<void> {
    const aggregateFunction = aggregate(firestore, testSecrets());
    await aggregateFunction.run({
      scheduleTime: new Date(scheduleTimeMs).toISOString(),
    });
  }

  test('queries the correct hour window', async () => {
    const fetcher = new FetchMocker(jest).result('POST', '/', {
      meta: [],
      data: [],
      rows: 0,
      // eslint-disable-next-line @typescript-eslint/naming-convention
      rows_before_limit_at_least: 0,
    });

    await runAggregate(Date.UTC(2024, 0, 25, 16, 1));
    await runAggregate(Date.UTC(2024, 0, 25, 16, 5));
    await runAggregate(Date.UTC(2024, 0, 25, 17, 1));

    expect(fetcher.requests()).toEqual([
      [
        'POST',
        'https://api.cloudflare.com/client/v4/accounts/foo-cloudflare-account/analytics_engine/sql',
      ],
      [
        'POST',
        'https://api.cloudflare.com/client/v4/accounts/foo-cloudflare-account/analytics_engine/sql',
      ],
      [
        'POST',
        'https://api.cloudflare.com/client/v4/accounts/foo-cloudflare-account/analytics_engine/sql',
      ],
    ]);
    expect(
      fetcher.bodys().map(query => String(query).replaceAll(/\s+/g, ' ')),
    ).toEqual([
      'SELECT teamID, appID, SUM(elapsed) AS totalElapsed, SUM(adjustedPeriod) AS totalPeriod FROM ( SELECT blob1 AS teamID, blob2 AS appID, blob3 AS roomID, double1 AS elapsed, double2 AS period, timestamp, IF(period > elapsed, elapsed, period) AS adjustedPeriod FROM RunningConnectionSeconds WHERE (timestamp >= toDateTime(1706194800)) AND (timestamp < toDateTime(1706198400)) ) GROUP BY teamID, appID FORMAT JSON',
      'SELECT teamID, appID, SUM(elapsed) AS totalElapsed, SUM(adjustedPeriod) AS totalPeriod FROM ( SELECT blob1 AS teamID, blob2 AS appID, blob3 AS roomID, double1 AS elapsed, double2 AS period, timestamp, IF(period > elapsed, elapsed, period) AS adjustedPeriod FROM RunningConnectionSeconds WHERE (timestamp >= toDateTime(1706194800)) AND (timestamp < toDateTime(1706198400)) ) GROUP BY teamID, appID FORMAT JSON',
      'SELECT teamID, appID, SUM(elapsed) AS totalElapsed, SUM(adjustedPeriod) AS totalPeriod FROM ( SELECT blob1 AS teamID, blob2 AS appID, blob3 AS roomID, double1 AS elapsed, double2 AS period, timestamp, IF(period > elapsed, elapsed, period) AS adjustedPeriod FROM RunningConnectionSeconds WHERE (timestamp >= toDateTime(1706198400)) AND (timestamp < toDateTime(1706202000)) ) GROUP BY teamID, appID FORMAT JSON',
    ]);

    // No aggregations should remain in Firestore
    const aggregations = await firestore
      .collection(AGGREGATIONS)
      .listDocuments();
    expect(aggregations).toHaveLength(0);
  });

  test('leaves aggregation upon failure', async () => {
    const fetcher = new FetchMocker(jest); // Defaults to 404

    const result = await runAggregate(Date.UTC(2024, 0, 25, 16, 1)).catch(
      e => e,
    );
    expect(result).toBeInstanceOf(Error);

    expect(fetcher.requests()).toEqual([
      [
        'POST',
        'https://api.cloudflare.com/client/v4/accounts/foo-cloudflare-account/analytics_engine/sql',
      ],
    ]);
    expect(
      fetcher.bodys().map(query => String(query).replaceAll(/\s+/g, ' ')),
    ).toEqual([
      'SELECT teamID, appID, SUM(elapsed) AS totalElapsed, SUM(adjustedPeriod) AS totalPeriod FROM ( SELECT blob1 AS teamID, blob2 AS appID, blob3 AS roomID, double1 AS elapsed, double2 AS period, timestamp, IF(period > elapsed, elapsed, period) AS adjustedPeriod FROM RunningConnectionSeconds WHERE (timestamp >= toDateTime(1706194800)) AND (timestamp < toDateTime(1706198400)) ) GROUP BY teamID, appID FORMAT JSON',
    ]);

    const aggregations = await firestore
      .collection(AGGREGATIONS)
      .listDocuments();
    expect(aggregations.map(doc => doc.id)).toEqual([
      '2024-01-25T16:00:00.000Z',
    ]);
  });

  test('retries previous aggregations (success)', async () => {
    const fetcher = new FetchMocker(jest).result('POST', '/', {
      meta: [],
      data: [],
      rows: 0,
      // eslint-disable-next-line @typescript-eslint/naming-convention
      rows_before_limit_at_least: 0,
    });

    // Previous attempt.
    await firestore
      .doc(aggregationPath(new Date('2024-01-25T16:00:00.000Z')))
      .create({});

    await runAggregate(Date.UTC(2024, 0, 25, 17, 1));

    expect(fetcher.requests()).toEqual([
      [
        'POST',
        'https://api.cloudflare.com/client/v4/accounts/foo-cloudflare-account/analytics_engine/sql',
      ],
      [
        'POST',
        'https://api.cloudflare.com/client/v4/accounts/foo-cloudflare-account/analytics_engine/sql',
      ],
    ]);
    expect(
      fetcher.bodys().map(query => String(query).replaceAll(/\s+/g, ' ')),
    ).toEqual([
      'SELECT teamID, appID, SUM(elapsed) AS totalElapsed, SUM(adjustedPeriod) AS totalPeriod FROM ( SELECT blob1 AS teamID, blob2 AS appID, blob3 AS roomID, double1 AS elapsed, double2 AS period, timestamp, IF(period > elapsed, elapsed, period) AS adjustedPeriod FROM RunningConnectionSeconds WHERE (timestamp >= toDateTime(1706194800)) AND (timestamp < toDateTime(1706198400)) ) GROUP BY teamID, appID FORMAT JSON',
      'SELECT teamID, appID, SUM(elapsed) AS totalElapsed, SUM(adjustedPeriod) AS totalPeriod FROM ( SELECT blob1 AS teamID, blob2 AS appID, blob3 AS roomID, double1 AS elapsed, double2 AS period, timestamp, IF(period > elapsed, elapsed, period) AS adjustedPeriod FROM RunningConnectionSeconds WHERE (timestamp >= toDateTime(1706198400)) AND (timestamp < toDateTime(1706202000)) ) GROUP BY teamID, appID FORMAT JSON',
    ]);

    const aggregations = await firestore
      .collection(AGGREGATIONS)
      .listDocuments();
    expect(aggregations).toHaveLength(0);
  });

  test('retries previous aggregations (failure)', async () => {
    const fetcher = new FetchMocker(jest); // Defaults to 404

    // Previous attempt.
    await firestore
      .doc(aggregationPath(new Date('2024-01-25T16:00:00.000Z')))
      .create({});

    const result = await runAggregate(Date.UTC(2024, 0, 25, 17, 1)).catch(
      e => e,
    );
    expect(result).toBeInstanceOf(Error);

    expect(fetcher.requests()).toEqual([
      [
        'POST',
        'https://api.cloudflare.com/client/v4/accounts/foo-cloudflare-account/analytics_engine/sql',
      ],
    ]);
    expect(
      fetcher.bodys().map(query => String(query).replaceAll(/\s+/g, ' ')),
    ).toEqual([
      'SELECT teamID, appID, SUM(elapsed) AS totalElapsed, SUM(adjustedPeriod) AS totalPeriod FROM ( SELECT blob1 AS teamID, blob2 AS appID, blob3 AS roomID, double1 AS elapsed, double2 AS period, timestamp, IF(period > elapsed, elapsed, period) AS adjustedPeriod FROM RunningConnectionSeconds WHERE (timestamp >= toDateTime(1706194800)) AND (timestamp < toDateTime(1706198400)) ) GROUP BY teamID, appID FORMAT JSON',
    ]);

    const aggregations = await firestore
      .collection(AGGREGATIONS)
      .listDocuments();
    expect(aggregations.map(doc => doc.id)).toEqual([
      '2024-01-25T16:00:00.000Z',
      '2024-01-25T17:00:00.000Z',
    ]);
  });

  test('retries previous aggregations (partial failure)', async () => {
    const fetcher = new FetchMocker(jest)
      .result('POST', '/', {
        meta: [],
        data: [],
        rows: 0,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        rows_before_limit_at_least: 0,
      })
      .once(); // Return one successful result, and fail the rest.

    // Previous attempt.
    await firestore
      .doc(aggregationPath(new Date('2024-01-25T16:00:00.000Z')))
      .create({});

    const result = await runAggregate(Date.UTC(2024, 0, 25, 17, 1)).catch(
      e => e,
    );
    expect(result).toBeInstanceOf(Error);

    expect(fetcher.requests()).toEqual([
      [
        'POST',
        'https://api.cloudflare.com/client/v4/accounts/foo-cloudflare-account/analytics_engine/sql',
      ],
      [
        'POST',
        'https://api.cloudflare.com/client/v4/accounts/foo-cloudflare-account/analytics_engine/sql',
      ],
    ]);
    expect(
      fetcher.bodys().map(query => String(query).replaceAll(/\s+/g, ' ')),
    ).toEqual([
      'SELECT teamID, appID, SUM(elapsed) AS totalElapsed, SUM(adjustedPeriod) AS totalPeriod FROM ( SELECT blob1 AS teamID, blob2 AS appID, blob3 AS roomID, double1 AS elapsed, double2 AS period, timestamp, IF(period > elapsed, elapsed, period) AS adjustedPeriod FROM RunningConnectionSeconds WHERE (timestamp >= toDateTime(1706194800)) AND (timestamp < toDateTime(1706198400)) ) GROUP BY teamID, appID FORMAT JSON',
      'SELECT teamID, appID, SUM(elapsed) AS totalElapsed, SUM(adjustedPeriod) AS totalPeriod FROM ( SELECT blob1 AS teamID, blob2 AS appID, blob3 AS roomID, double1 AS elapsed, double2 AS period, timestamp, IF(period > elapsed, elapsed, period) AS adjustedPeriod FROM RunningConnectionSeconds WHERE (timestamp >= toDateTime(1706198400)) AND (timestamp < toDateTime(1706202000)) ) GROUP BY teamID, appID FORMAT JSON',
    ]);

    const aggregations = await firestore
      .collection(AGGREGATIONS)
      .listDocuments();
    expect(aggregations.map(doc => doc.id)).toEqual([
      '2024-01-25T17:00:00.000Z', // The retried attempt succeeded, but the new attempt remains.
    ]);
  });
});
