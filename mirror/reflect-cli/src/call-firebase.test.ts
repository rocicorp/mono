import {expect, jest, test} from '@jest/globals';
import * as v from 'shared/src/valita.js';
import {callFirebase} from './call-firebase.js';

function makeFetchSpy(
  body: BodyInit | null | undefined,
  init?: ResponseInit | undefined,
) {
  const fetchSpy = jest.spyOn(globalThis, 'fetch');
  fetchSpy.mockImplementationOnce(url => {
    expect(url).toMatch(/\/unknown$/);
    return Promise.resolve(new Response(body, init));
  });
  return fetchSpy;
}

test('it should throw if response is not ok', async () => {
  makeFetchSpy('+null', {status: 500, statusText: 'NOT JSON'});
  await expect(callFirebase('unknown', {})).rejects.toMatchInlineSnapshot(
    `[Error: Unexpected response from Firebase: 500: NOT JSON]`,
  );
});

test('it should throw if response contains error', async () => {
  makeFetchSpy(
    JSON.stringify({
      error: {
        message: 'MESSAGE',
        status: 'STATUS',
      },
    }),
  );

  await expect(callFirebase('unknown', {})).rejects.toMatchInlineSnapshot(
    `[FirebaseError: STATUS, MESSAGE]`,
  );
});

test('it should throw if response contains error with details', async () => {
  makeFetchSpy(
    JSON.stringify({
      error: {
        message: 'MESSAGE',
        status: 'STATUS',
        details: 'DETAILS',
      },
    }),
  );
  await expect(callFirebase('unknown', {})).rejects.toMatchInlineSnapshot(
    `[FirebaseError: STATUS, MESSAGE, DETAILS]`,
  );
});

test('it should throw if response is not valid shape', async () => {
  makeFetchSpy(
    JSON.stringify({
      foo: 42,
    }),
  );
  await expect(callFirebase('unknown', {})).rejects.toMatchInlineSnapshot(
    `[Error: Unexpected response from Firebase: {"foo":42}]`,
  );
});

test('it should throw if response is not JSON', async () => {
  makeFetchSpy('x');
  await expect(callFirebase('unknown', {})).rejects.toMatchInlineSnapshot(
    `[Error: Unexpected response from Firebase. Invalid JSON: Unexpected token x in JSON at position 0]`,
  );
});

test('it should return result', async () => {
  makeFetchSpy(
    JSON.stringify({
      result: {
        foo: 42,
      },
    }),
  );
  await expect(callFirebase('unknown', {})).resolves.toEqual({
    foo: 42,
  });
});

test('it should return result if matches schema', async () => {
  makeFetchSpy(
    JSON.stringify({
      result: [1, 2, 3],
    }),
  );
  const schema = v.array(v.number());
  await expect(callFirebase('unknown', {}, schema)).resolves.toEqual([1, 2, 3]);
});

test('it should throw if result does not match schema', async () => {
  makeFetchSpy(
    JSON.stringify({
      result: [1, 2, 3],
    }),
  );
  const schema = v.array(v.string());
  await expect(
    callFirebase('unknown', {}, schema),
  ).rejects.toMatchInlineSnapshot(`[TypeError: Expected string at 0. Got 1]`);
});
