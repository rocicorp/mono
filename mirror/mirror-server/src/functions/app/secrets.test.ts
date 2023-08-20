import {describe, expect, test} from '@jest/globals';
import {hashSecrets} from './secrets.js';

describe('secrets', () => {
  test('hashing', async () => {
    /* eslint-disable @typescript-eslint/naming-convention */
    const secrets = {
      REFLECT_AUTH_API_KEY: 'foo',
      DATADOG_LOGS_API_KEY: 'bar',
      DATADOG_METRICS_API_KEY: 'baz',
    };
    expect(await hashSecrets(secrets)).toEqual({
      REFLECT_AUTH_API_KEY:
        '2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae',
      DATADOG_LOGS_API_KEY:
        'fcde2b2edba56bf40601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9',
      DATADOG_METRICS_API_KEY:
        'baa5a0964d3320fbc0c6a92214053c8513ea24ab8fd0770480a967248096',
    });
    expect(secrets).toEqual({
      REFLECT_AUTH_API_KEY: 'foo',
      DATADOG_LOGS_API_KEY: 'bar',
      DATADOG_METRICS_API_KEY: 'baz',
    });
    /* eslint-enable @typescript-eslint/naming-convention */
  });
});
