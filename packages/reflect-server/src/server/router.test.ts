import {describe, expect, test} from '@jest/globals';
import {assert} from 'shared/src/asserts.js';
import type {JSONObject, ReadonlyJSONValue} from 'shared/src/json.js';
import {must} from 'shared/src/must.js';
import * as valita from 'shared/src/valita.js';
import type {ListOptions} from '../storage/storage.js';
import {createSilentLogContext} from '../util/test-utils.js';
import type {APIErrorInfo} from './api-response.js';
import {HttpError} from './errors.js';
import {
  BaseContext,
  Handler,
  Router,
  body,
  checkAuthAPIKey,
  get,
  listControl,
  post,
  requiredAuthAPIKey,
  roomID,
  urlVersion,
  userID,
} from './router.js';

test('Router', async () => {
  const router = new Router();
  router.register(
    '/foo/:id',
    ctx =>
      new Response(`foo:${ctx.parsedURL.pathname.groups.id}`, {
        status: 200,
      }),
  );
  router.register(
    '/bar/:id',
    ctx =>
      new Response(`bar:${ctx.parsedURL.pathname.groups.id}`, {
        status: 400,
      }),
  );
  router.register(
    '/bar',
    () =>
      new Response(`bar`, {
        status: 500,
      }),
  );

  type Case = {
    path: string;
    expectedResponseCode: number | undefined;
    expectedResponseText?: string;
    expectedResponseJSON?: ReadonlyJSONValue;
  };

  const cases: Case[] = [
    {
      path: '/foo/42',
      expectedResponseCode: 200,
      expectedResponseText: 'foo:42',
    },
    {
      path: '/bar/44',
      expectedResponseCode: 400,
      expectedResponseText: 'bar:44',
    },
    {
      path: '/bar',
      expectedResponseCode: 500,
      expectedResponseText: 'bar',
    },
    {
      path: '/monkey/nuts',
      expectedResponseCode: 404,
      expectedResponseJSON: {
        result: null,
        error: {
          code: 404,
          resource: 'request',
          message: 'Unknown or invalid URL',
        },
      },
    },
  ];
  for (const c of cases) {
    const resp = await router.dispatch(
      new Request(`https://test.roci.dev${c.path}`),
      {lc: createSilentLogContext()},
    );

    expect(resp?.status).toEqual(c.expectedResponseCode);
    if (c.expectedResponseText) {
      expect(await resp?.text()).toEqual(c.expectedResponseText);
    } else if (c.expectedResponseJSON) {
      expect(await resp?.json()).toEqual(c.expectedResponseJSON);
    }
  }
});

test('requireMethod', async () => {
  const getHandler = get().handle(
    (_, req) => new Response(`${req.url}`, {status: 300}),
  );
  const postHandler = post().handle(
    async (_, req) =>
      new Response(`${req.url}:${await req.text()}`, {status: 301}),
  );

  const pattern = new URLPattern();
  const url = 'https://test.roci.dev/';
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const parsedURL = pattern.exec(url)!;

  type Case = {
    handler: Handler<BaseContext, Response>;
    method: string;
    expectedStatus: number;
    expectedText?: string;
    expectedJSON?: ReadonlyJSONValue;
  };

  const cases: Case[] = [
    {
      handler: getHandler,
      method: 'GET',
      expectedStatus: 300,
      expectedText: url,
    },
    {
      handler: getHandler,
      method: 'POST',
      expectedStatus: 405,
      expectedJSON: {
        result: null,
        error: {
          code: 405,
          resource: 'request',
          message: 'unsupported method',
        },
      },
    },
    {
      handler: postHandler,
      method: 'POST',
      expectedStatus: 301,
      expectedText: `${url}:ok`,
    },
    {
      handler: postHandler,
      method: 'GET',
      expectedStatus: 405,
      expectedJSON: {
        result: null,
        error: {
          code: 405,
          resource: 'request',
          message: 'unsupported method',
        },
      },
    },
  ];

  const lc = createSilentLogContext();

  for (const c of cases) {
    const req = new Request(url, {
      method: c.method,
      body: c.method === 'POST' ? 'ok' : null,
    });

    const resp = await c.handler({lc, parsedURL}, req);
    expect(resp.status).toBe(c.expectedStatus);
    if (c.expectedText) {
      expect(await resp.text()).toBe(c.expectedText);
    } else if (c.expectedJSON) {
      expect(await resp.json()).toEqual(c.expectedJSON);
    }
  }
});

describe('checkAuthAPIKey', () => {
  type Case = {
    name: string;
    required: string;
    headers: Record<string, string>;
    expectedError?: string;
  };

  const cases: Case[] = [
    {
      name: 'required key cannot be empty even if actual key is not sent in the headers',
      required: '',
      headers: {},
      expectedError:
        'Error: Internal error: expected auth api key cannot be empty',
    },
    {
      name: 'required key cannot be empty even if actual is the same empty key',
      required: '',
      headers: {
        ['x-reflect-api-key']: '',
      },
      expectedError:
        'Error: Internal error: expected auth api key cannot be empty',
    },
    {
      name: 'required key cannot be empty, even if actual key is provided',
      required: '',
      headers: {
        ['x-reflect-api-key']: 'foo',
      },
      expectedError:
        'Error: Internal error: expected auth api key cannot be empty',
    },
    {
      name: 'no api key sent',
      required: 'foo',
      headers: {},
      expectedError: 'Error: Unauthorized',
    },
    {
      name: 'empty api key sent',
      required: 'foo',
      headers: {
        ['x-reflect-api-key']: '',
      },
      expectedError: 'Error: Unauthorized',
    },
    {
      name: 'wrong api key sent',
      required: 'foo',
      headers: {
        ['x-reflect-api-key']: 'bar',
      },
      expectedError: 'Error: Unauthorized',
    },
    {
      name: 'wrong legacy api key sent',
      required: 'foo',
      headers: {
        ['x-reflect-auth-api-key']: 'bar',
      },
      expectedError: 'Error: Unauthorized',
    },
    {
      name: 'correct api key sent',
      required: 'foo',
      headers: {
        ['x-reflect-api-key']: 'foo',
      },
    },
    {
      name: 'legacy api key sent',
      required: 'foo',
      headers: {
        ['x-reflect-auth-api-key']: 'foo',
      },
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      const {headers} = c;
      try {
        checkAuthAPIKey(
          c.required,
          new Request('https://roci.dev/', {
            headers,
          }),
        );
        expect(c.expectedError).toBeUndefined;
      } catch (e) {
        expect(String(e)).toEqual(c.expectedError);
      }
    });
  }
});

test('requireAuthAPIKey', async () => {
  type Case = {
    required: string;
    actual: string | null;
    text: string;
    status?: number;
  };

  const cases: Case[] = [
    {
      required: '',
      actual: null,
      text: 'Internal error: expected auth api key cannot be empty',
      status: 500,
    },
    {
      required: '',
      actual: '',
      text: 'Internal error: expected auth api key cannot be empty',
      status: 500,
    },
    {
      required: '',
      actual: 'foo',
      text: 'Internal error: expected auth api key cannot be empty',
      status: 500,
    },
    {
      required: 'foo',
      actual: null,
      text: 'Unauthorized',
      status: 401,
    },
    {
      required: 'foo',
      actual: '',
      text: 'Unauthorized',
      status: 401,
    },
    {
      required: 'foo',
      actual: 'bar',
      text: 'Unauthorized',
      status: 401,
    },
    {
      required: 'foo',
      actual: 'foo',
      text: 'ok',
      status: 200,
    },
  ];

  for (const c of cases) {
    const headers: Record<string, string> = {};
    if (c.actual !== null) {
      headers['x-reflect-api-key'] = c.actual;
    }
    const handler = post()
      .with(requiredAuthAPIKey(() => c.required))
      .handle(async (_, req) => new Response(await req.text(), {status: 200}));

    const req = new Request('https://roci.dev/', {
      method: 'POST',
      body: 'ok',
      headers,
    });
    const ctx = {
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      parsedURL: new URLPattern().exec()!,
      lc: createSilentLogContext(),
    };
    const response = await handler(ctx, req);
    expect(await response.text()).toBe(c.text);
    expect(response.status).toBe(c.status ?? 200);
  }
});

test('withRoomID', async () => {
  type Case = {
    parsedURL: URLPatternURLPatternResult;
    expected: {result: {text: string; status: number}};
  };

  const cases: Case[] = [
    {
      parsedURL: must(
        new URLPattern({pathname: '/room/:roomID'}).exec(
          'https://roci.dev/room/monkey',
        ),
      ),
      expected: {result: {text: 'roomID:monkey', status: 200}},
    },
    {
      parsedURL: must(
        new URLPattern({pathname: '/room/:roomID'}).exec(
          'https://roci.dev/room/%24',
        ),
      ),
      expected: {result: {text: 'roomID:$', status: 200}},
    },
    {
      parsedURL: must(
        new URLPattern({pathname: '/room/:roomID/x'}).exec(
          'https://roci.dev/room/a%2Fb/x',
        ),
      ),
      expected: {result: {text: 'roomID:a/b', status: 200}},
    },
    {
      parsedURL: must(
        new URLPattern({pathname: '/room/:otherThing'}).exec(
          'https://roci.dev/room/monkey',
        ),
      ),
      expected: {
        result: {
          text: 'roomID() configured for URL without :roomID group',
          status: 500,
        },
      },
    },
  ];

  const handler = get()
    .with(roomID())
    .handle(ctx => new Response(`roomID:${ctx.roomID}`, {status: 200}));

  for (const c of cases) {
    const url = `https://roci.dev/`;
    const request = new Request(url);
    const ctx = {
      parsedURL: c.parsedURL,
      lc: createSilentLogContext(),
    };

    const response = await handler(ctx, request);
    const result = {
      result: {status: response.status, text: await response.text()},
    };

    expect(result).toEqual(c.expected);
  }
});

test('withUserID', async () => {
  type Case = {
    parsedURL: URLPatternURLPatternResult;
    expected?: {result: {text: string; status: number}};
  };

  const cases: Case[] = [
    {
      parsedURL: must(
        new URLPattern({pathname: '/users/:userID'}).exec(
          'https://roci.dev/users/monkey',
        ),
      ),
      expected: {result: {text: 'userID:monkey', status: 200}},
    },
    {
      parsedURL: must(
        new URLPattern({pathname: '/users/:userID'}).exec(
          'https://roci.dev/users/%24',
        ),
      ),
      expected: {result: {text: 'userID:$', status: 200}},
    },
    {
      parsedURL: must(
        new URLPattern({pathname: '/users/:userID/x'}).exec(
          'https://roci.dev/users/a%2Fb/x',
        ),
      ),
      expected: {result: {text: 'userID:a/b', status: 200}},
    },
    {
      parsedURL: must(
        new URLPattern({pathname: '/users/:otherThing'}).exec(
          'https://roci.dev/users/monkey',
        ),
      ),
      expected: {
        result: {
          text: 'userID() configured for URL without :userID group',
          status: 500,
        },
      },
    },
  ];

  const handler = get()
    .with(userID())
    .handle(ctx => new Response(`userID:${ctx.userID}`, {status: 200}));

  for (const c of cases) {
    const url = `https://roci.dev/`;
    const request = new Request(url);
    const ctx = {
      parsedURL: c.parsedURL,
      lc: createSilentLogContext(),
    };

    const response = await handler(ctx, request);
    const result = {
      result: {status: response.status, text: await response.text()},
    };

    expect(result).toEqual(c.expected);
  }
});

test('withBody', async () => {
  type Case = {
    body: JSONObject | undefined | string;
    expected?: {text: string; status: number};
    error?: APIErrorInfo;
  };

  const cases: Case[] = [
    {
      body: {userID: 'foo'},
      expected: {text: 'userID:foo', status: 200},
    },
    {
      body: {badUserId: 'bar'},
      error: {
        code: 400,
        resource: 'request',
        message: 'Body schema error. Missing property userID',
      },
    },
    {
      body: {userID: 'foo', badUserId: 'bar'},
      error: {
        code: 400,
        resource: 'request',
        message: 'Body schema error. Unexpected property badUserId',
      },
    },
    {
      body: undefined,
      error: {
        code: 400,
        resource: 'request',
        message: 'Body must be valid json.',
      },
    },
    {
      body: 'foo',
      error: {
        code: 400,
        resource: 'request',
        message: 'Body schema error. Expected object. Got "foo"',
      },
    },
  ];

  const userIdSchema = valita.object({userID: valita.string()});
  const handler = post()
    .with(body(userIdSchema))
    .handle(ctx => {
      const {body} = ctx;
      const {userID} = body;
      return new Response(`userID:${userID}`, {status: 200});
    });

  for (const c of cases) {
    const url = `https://roci.dev/`;
    const request = new Request(url, {
      method: 'post',
      body: JSON.stringify(c.body),
    });
    const ctx = {
      lc: createSilentLogContext(),
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      parsedURL: new URLPattern().exec()!,
    };

    const response = await handler(ctx, request);
    if (response.status === 200) {
      expect({status: response.status, text: await response.text()}).toEqual(
        c.expected,
      );
    } else {
      expect(response.status).toBe(c.error?.code);
      expect(await response.json()).toEqual({
        result: null,
        error: c.error,
      });
    }
  }
});

describe('withListControl', () => {
  type Case = {
    queryString: string;
    listOptions?: ListOptions;
    error?: APIErrorInfo;
  };

  const cases: Case[] = [
    {
      queryString: '',
      listOptions: {start: {key: '', exclusive: false}, limit: 101},
    },
    {
      queryString: 'startKey=foo',
      listOptions: {start: {key: 'foo', exclusive: false}, limit: 101},
    },
    {
      queryString: 'startAfterKey=bar',
      listOptions: {start: {key: 'bar', exclusive: true}, limit: 101},
    },
    {
      queryString: 'maxResults=10',
      listOptions: {start: {key: '', exclusive: false}, limit: 11},
    },
    {
      queryString: 'maxResults=90',
      listOptions: {start: {key: '', exclusive: false}, limit: 91},
    },
    {
      queryString: 'maxResults=200',
      listOptions: {start: {key: '', exclusive: false}, limit: 101},
    },
    {
      queryString: 'maxResults=90&startKey=bonk',
      listOptions: {start: {key: 'bonk', exclusive: false}, limit: 91},
    },
    {
      queryString: 'maxResults=not-a-number',
      error: {
        code: 400,
        resource: 'request',
        message: 'Expected valid number at maxResults. Got "not-a-number"',
      },
    },
    {
      queryString: 'startKey=and&startAfterKey=not-allowed',
      error: {
        code: 400,
        resource: 'request',
        message: 'Cannot specify both startKey and startAfterKey. Got object',
      },
    },
  ];

  const handler = get()
    .with(listControl(100))
    .handleJSON(ctx => {
      const {listControl} = ctx;
      return listControl.getOptions();
    });

  for (const c of cases) {
    test(c.queryString, async () => {
      const url = `https://roci.dev/?${c.queryString}`;
      const request = new Request(url);
      const ctx = {
        lc: createSilentLogContext(),
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        parsedURL: new URLPattern().exec(url)!,
      };

      const response = await handler(ctx, request);
      if (response.status === 200) {
        expect(await response.json()).toEqual(c.listOptions);
      } else {
        expect(response.status).toBe(c.error?.code);
        expect(await response.json()).toEqual({
          error: c.error,
          result: null,
        });
      }
    });
  }
});

test('handleJSON', async () => {
  type Case = {
    input: string;
    expected: ReadonlyJSONValue;
  };

  const cases: Case[] = [
    {
      input: 'bar',
      expected: {
        foo: 'bar',
      },
    },
    {
      input: 'monkey',
      expected: {
        foo: 'monkey',
      },
    },
  ];

  for (const c of cases) {
    const handler = post().handleJSON(async (_, req) => ({
      foo: await req.text(),
    }));
    const request = new Request('http://roci.dev/', {
      method: 'POST',
      body: c.input,
    });
    const ctx = {
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      parsedURL: new URLPattern().exec('https://roci.dev/')!,
      lc: createSilentLogContext(),
    };
    const response = await handler(ctx, request);
    expect(await response.json()).toEqual(c.expected);
  }
});

test('handleAPIResult', async () => {
  type Case = {
    input: string;
    expected: ReadonlyJSONValue;
  };

  const cases: Case[] = [
    {
      input: 'bar',
      expected: {
        foo: 'bar',
      },
    },
    {
      input: 'monkey',
      expected: {
        foo: 'monkey',
      },
    },
  ];

  for (const c of cases) {
    const handler = post().handleAPIResult(async (_, req) => ({
      foo: await req.text(),
    }));
    const request = new Request('http://roci.dev/', {
      method: 'POST',
      body: c.input,
    });
    const ctx = {
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      parsedURL: new URLPattern().exec('https://roci.dev/')!,
      lc: createSilentLogContext(),
    };
    const response = await handler(ctx, request);
    expect(await response.json()).toEqual({
      result: c.expected,
      error: null,
    });
  }
});

test('withVersion', async () => {
  const t = async (path: string, exp: {text: string; status: number}) => {
    const url = `https://roci.dev`;
    const request = new Request(url);
    const parsedURL = new URLPattern({
      pathname: '/version/:version/monkey',
    }).exec(url + path);
    assert(parsedURL);
    const ctx = {
      parsedURL,
      lc: createSilentLogContext(),
    };

    const handler = get()
      .with(urlVersion())
      .handle(ctx => new Response(`version:${ctx.version}`, {status: 200}));

    const response = await handler(ctx, request);
    const result = {
      status: response.status,
      text: await response.text(),
    } as const;

    expect(result).toEqual(exp);
  };

  await t('/version/v0/monkey', {text: 'version:0', status: 200});
  await t('/version/v1/monkey', {text: 'version:1', status: 200});
  await t('/version/v234/monkey', {text: 'version:234', status: 200});
  await t('/version/v01/monkey', {text: 'version:1', status: 200});

  await t('/version/1/monkey', {
    text: 'invalid version found by withVersion, 1',
    status: 500,
  });
  await t('/version/123/monkey', {
    text: 'invalid version found by withVersion, 123',
    status: 500,
  });
  await t('/version/123v/monkey', {
    text: 'invalid version found by withVersion, 123v',
    status: 500,
  });
  await t('/version/bananas/monkey', {
    text: 'invalid version found by withVersion, bananas',
    status: 500,
  });
});

test('handleErrors', async () => {
  const ctx = {
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    parsedURL: new URLPattern().exec('https://roci.dev/')!,
    lc: createSilentLogContext(),
  };

  const handler = get().handle(() => {
    throw new HttpError(401, 'foo');
  });
  let response = await handler(ctx, new Request('https://roci.dev/'));
  expect(response.status).toBe(401);
  expect(await response.text()).toBe('foo');

  const jsonHandler = get().handleJSON(() => {
    throw new HttpError(402, 'bar');
  });
  response = await jsonHandler(ctx, new Request('https://roci.dev/'));
  expect(response.status).toBe(402);
  expect(await response.text()).toBe('bar');

  const apiHandler = get().handleAPIResult(() => {
    throw new HttpError(403, 'bonk');
  });
  response = await apiHandler(ctx, new Request('https://roci.dev/'));
  expect(response.status).toBe(403);
  expect(await response.text()).toBe('bonk');
});
