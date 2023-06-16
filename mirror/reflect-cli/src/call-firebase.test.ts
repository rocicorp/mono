import {expect, jest, test} from '@jest/globals';
import * as v from 'shared/valita.js';
import {callFirebase} from './call-firebase.js';

function makeFetchSpy(
  body: BodyInit | null | undefined,
  init?: ResponseInit | undefined,
) {
  const fetchSpy = jest.spyOn(globalThis, 'fetch');
  fetchSpy.mockImplementationOnce(url => {
    expect(url).toMatch(/\/publish$/);
    return Promise.resolve(new Response(body, init));
  });
  return fetchSpy;
}

test('it should throw if response is not ok', async () => {
  makeFetchSpy(null, {status: 500, statusText: 'NOT OK'});
  await expect(callFirebase('publish', {})).rejects.toMatchInlineSnapshot(
    `[Error: HTTP error 500: NOT OK]`,
  );
});

test('it should throw if response contains error', async () => {
  makeFetchSpy(
    JSON.stringify({
      error: {
        message: 'NOT OK',
        status: 501,
      },
    }),
  );

  await expect(callFirebase('publish', {})).rejects.toMatchInlineSnapshot(
    `[Error: Firebase error 501: NOT OK]`,
  );
});

test('it should throw if response contains error with details', async () => {
  makeFetchSpy(
    JSON.stringify({
      error: {
        message: 'NOT OK',
        status: 501,
        details: 'DETAILS',
      },
    }),
  );
  await expect(callFirebase('publish', {})).rejects.toMatchInlineSnapshot(
    `[Error: Firebase error 501: NOT OK, DETAILS]`,
  );
});

test('it should throw if response is not valid shape', async () => {
  makeFetchSpy(
    JSON.stringify({
      foo: 42,
    }),
  );
  await expect(callFirebase('publish', {})).rejects.toMatchInlineSnapshot(
    `[Error: Unexpected response from Firebase: {"foo":42}]`,
  );
});

test('it should throw if response is not JSON', async () => {
  makeFetchSpy('x');
  await expect(callFirebase('publish', {})).rejects.toMatchInlineSnapshot(
    `[SyntaxError: Unexpected token x in JSON at position 0]`,
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
  await expect(callFirebase('publish', {})).resolves.toEqual({
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
  await expect(callFirebase('publish', {}, schema)).resolves.toEqual([1, 2, 3]);
});

test('it should throw if result does not match schema', async () => {
  makeFetchSpy(
    JSON.stringify({
      result: [1, 2, 3],
    }),
  );
  const schema = v.array(v.string());
  await expect(
    callFirebase('publish', {}, schema),
  ).rejects.toMatchInlineSnapshot(`[TypeError: Expected string at 0. Got 1]`);
});
