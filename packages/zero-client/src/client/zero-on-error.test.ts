import {expect, test, vi} from 'vitest';
import {sleep} from '../../../shared/src/sleep.ts';
import * as ErrorKind from '../../../zero-protocol/src/error-kind-enum.ts';
import {ServerError} from './server-error.ts';
import {zeroForTest} from './test-utils.ts'; // Why use fakes when we can use the real thing!

test('onError should be called if present', async () => {
  const onError = vi.fn();
  const z = zeroForTest({
    onError,
  });

  await z.triggerConnected();

  await z.triggerError(ErrorKind.InvalidConnectionRequest, 'test');

  await sleep(1);

  expect(onError.mock.calls).toEqual([
    [
      {
        clientID: expect.any(String),
        runLoopCounter: 2,
        wsid: expect.any(String),
      },
      'Failed to connect',
      new ServerError({
        kind: ErrorKind.InvalidConnectionRequest,
        message: 'test',
      }),
      {
        baseCookie: null,
        lmid: 0,
      },
    ],
  ]);
});
