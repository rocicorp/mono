import {test, expect} from '@jest/globals';
import type {JSONObject, ReadonlyJSONValue} from 'replicache';
import {createSilentLogContext} from '../util/test-utils.js';
import {
  asJSON,
  BaseContext,
  checkAuthAPIKey,
  get,
  Handler,
  post,
  requireAuthAPIKey,
  Router,
  withBody,
  withRoomID,
  withVersion,
} from './router.js';
import * as s from 'superstruct';
import {assert} from 'shared';
import {must} from '../util/must.js';

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
    expectedError?: string | undefined;
    expectedResponseCode: number | undefined;
    expectedResponseText: string | undefined;
  };

  const cases: Case[] = [
    {
      path: '/foo/42',
      expectedError: undefined,
      expectedResponseCode: 200,
      expectedResponseText: 'foo:42',
    },
    {
      path: '/bar/44',
      expectedError: undefined,
      expectedResponseCode: 400,
      expectedResponseText: 'bar:44',
    },
    {
      path: '/bar',
      expectedError: undefined,
      expectedResponseCode: 500,
      expectedResponseText: 'bar',
    },
    {
      path: '/monkey/nuts',
      expectedError: undefined,
      expectedResponseCode: 404,
      expectedResponseText: 'not found',
    },
  ];
  for (const c of cases) {
    let error: unknown;
    let resp: Response | undefined;
    try {
      resp = await router.dispatch(
        new Request(`https://test.roci.dev${c.path}`),
        {lc: createSilentLogContext()},
      );
    } catch (e) {
      error = e;
    }
    if (c.expectedError === undefined) {
      expect(error).toBeUndefined();
    } else {
      expect(String(error)).toMatch(c.expectedError);
    }
    expect(resp?.status).toEqual(c.expectedResponseCode);
    expect(await resp?.text()).toEqual(c.expectedResponseText);
  }
});

test('requireMethod', async () => {
  const getHandler = get((_, req) => new Response(`${req.url}`, {status: 300}));
  const postHandler = post(
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
    expectedText: string;
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
      expectedText: 'unsupported method',
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
      expectedText: 'unsupported method',
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
    expect(await resp.text()).toBe(c.expectedText);
  }
});

test('checkAuthAPIKey', async () => {
  type Case = {
    required: string;
    actual: string | null;
    expected:
      | {error: string}
      | {result: {text: string; status: number}}
      | undefined;
  };

  const cases: Case[] = [
    {
      required: '',
      actual: null,
      expected: {
        error: 'Error: Internal error: expected auth api key cannot be empty',
      },
    },
    {
      required: '',
      actual: '',
      expected: {
        error: 'Error: Internal error: expected auth api key cannot be empty',
      },
    },
    {
      required: '',
      actual: 'foo',
      expected: {
        error: 'Error: Internal error: expected auth api key cannot be empty',
      },
    },
    {
      required: 'foo',
      actual: null,
      expected: {result: {text: 'Unauthorized', status: 401}},
    },
    {
      required: 'foo',
      actual: '',
      expected: {result: {text: 'Unauthorized', status: 401}},
    },
    {
      required: 'foo',
      actual: 'bar',
      expected: {result: {text: 'Unauthorized', status: 401}},
    },
    {
      required: 'foo',
      actual: 'foo',
      expected: undefined,
    },
  ];

  for (const c of cases) {
    const headers: Record<string, string> = {};
    if (c.actual !== null) {
      headers['x-reflect-auth-api-key'] = c.actual;
    }

    let result: Case['expected'];

    try {
      const response = checkAuthAPIKey(
        c.required,
        new Request('https://roci.dev/', {
          headers,
        }),
      );
      if (response === undefined) {
        result = response;
      } else {
        result = {
          result: {status: response.status, text: await response.text()},
        };
      }
    } catch (e) {
      result = {error: String(e)};
    }

    expect(result).toEqual(c.expected);
  }
});

test('requireAuthAPIKey', async () => {
  type Case = {
    required: string;
    actual: string | null;
    expected: {error: string} | {result: {text: string; status: number}};
  };

  const cases: Case[] = [
    {
      required: '',
      actual: null,
      expected: {
        error: 'Error: Internal error: expected auth api key cannot be empty',
      },
    },
    {
      required: '',
      actual: '',
      expected: {
        error: 'Error: Internal error: expected auth api key cannot be empty',
      },
    },
    {
      required: '',
      actual: 'foo',
      expected: {
        error: 'Error: Internal error: expected auth api key cannot be empty',
      },
    },
    {
      required: 'foo',
      actual: null,
      expected: {result: {text: 'Unauthorized', status: 401}},
    },
    {
      required: 'foo',
      actual: '',
      expected: {result: {text: 'Unauthorized', status: 401}},
    },
    {
      required: 'foo',
      actual: 'bar',
      expected: {result: {text: 'Unauthorized', status: 401}},
    },
    {
      required: 'foo',
      actual: 'foo',
      expected: {result: {text: 'ok', status: 200}},
    },
  ];

  for (const c of cases) {
    const headers: Record<string, string> = {};
    if (c.actual !== null) {
      headers['x-reflect-auth-api-key'] = c.actual;
    }

    let result: Case['expected'] | undefined = undefined;

    const handler = requireAuthAPIKey(
      () => c.required,
      async (_, req) => new Response(await req.text(), {status: 200}),
    );

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
    try {
      const response = await handler(ctx, req);
      if (response === undefined) {
        result = response;
      } else {
        result = {
          result: {status: response.status, text: await response.text()},
        };
      }
    } catch (e) {
      result = {error: String(e)};
    }

    expect(result).toEqual(c.expected);
  }
});

test('withRoomID', async () => {
  type Case = {
    parsedURL: URLPatternURLPatternResult;
    expected: {result: {text: string; status: number}} | {error: string};
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
        error: 'Error: Internal error: roomID not found by withRoomID',
      },
    },
  ];

  const handler = withRoomID(
    ctx => new Response(`roomID:${ctx.roomID}`, {status: 200}),
  );

  for (const c of cases) {
    const url = `https://roci.dev/`;
    const request = new Request(url);
    const ctx = {
      parsedURL: c.parsedURL,
      lc: createSilentLogContext(),
    };

    let result: Case['expected'] | undefined = undefined;
    try {
      const response = await handler(ctx, request);
      result = {result: {status: response.status, text: await response.text()}};
    } catch (e) {
      result = {error: String(e)};
    }

    expect(result).toEqual(c.expected);
  }
});

test('withBody', async () => {
  type Case = {
    body: JSONObject | undefined | string;
    expected: {text: string; status: number} | {error: string};
  };

  const cases: Case[] = [
    {
      body: {userID: 'foo'},
      expected: {text: 'userID:foo', status: 200},
    },
    {
      body: {badUserId: 'bar'},
      expected: {
        status: 400,
        text: 'Body schema error. At path: userID -- Expected a string, but received: undefined',
      },
    },
    {
      body: undefined,
      expected: {
        status: 400,
        text: 'Body must be valid json.',
      },
    },
    {
      body: 'foo',
      expected: {
        status: 400,
        text: 'Body schema error. Expected an object, but received: "foo"',
      },
    },
  ];

  const userIdStruct = s.type({userID: s.string()});
  const handler = withBody(userIdStruct, ctx => {
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

    let result: Case['expected'] | undefined = undefined;
    try {
      const response = await handler(ctx, request);
      result = {status: response.status, text: await response.text()};
    } catch (e) {
      result = {error: String(e)};
    }

    expect(result).toEqual(c.expected);
  }
});

test('asJSON', async () => {
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
    const handler = asJSON(async (_, req) => ({
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

test('withVersion', async () => {
  const t = async (
    path: string,
    exp: {text: string; status: number} | {error: string},
  ) => {
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

    const handler = withVersion(
      ctx => new Response(`version:${ctx.version}`, {status: 200}),
    );

    let result: typeof exp;
    try {
      const response = await handler(ctx, request);
      result = {status: response.status, text: await response.text()} as const;
    } catch (e) {
      result = {error: String(e)} as const;
    }

    expect(result).toEqual(exp);
  };

  await t('/version/v0/monkey', {text: 'version:0', status: 200});
  await t('/version/v1/monkey', {text: 'version:1', status: 200});
  await t('/version/v234/monkey', {text: 'version:234', status: 200});
  await t('/version/v01/monkey', {text: 'version:1', status: 200});

  await t('/version/1/monkey', {
    error: 'Error: invalid version found by withVersion, 1',
  });
  await t('/version/123/monkey', {
    error: 'Error: invalid version found by withVersion, 123',
  });
  await t('/version/123v/monkey', {
    error: 'Error: invalid version found by withVersion, 123v',
  });
  await t('/version/bananas/monkey', {
    error: 'Error: invalid version found by withVersion, bananas',
  });
});
