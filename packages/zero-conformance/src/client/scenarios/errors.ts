/**
 * Error messages from zero-cache and what each does to the connection, and
 * resuming with connect().
 */
import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import type {Scenario, Step} from '../harness.ts';
import {advance, connected, errorMessage, mutate, start} from './helpers.ts';

type JSONObject = {readonly [key: string]: ReadonlyJSONValue};

/** Connect, then receive `body` while connected, then try to resume. */
function whileConnected(name: string, body: JSONObject, more: Step[] = []) {
  return {
    name: `error while connected: ${name}`,
    steps: [
      start,
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      errorMessage(body),
      advance(5000),
      mutate('m2', 'issue.create', {id: 'i2'}),
      ...more,
    ],
  };
}

export const errorScenarios: Scenario[] = [
  whileConnected(
    'AuthInvalidated moves to needs-auth until connect() with a new token',
    {kind: 'AuthInvalidated', message: 'token expired', origin: 'zeroCache'},
    [{op: 'connect', auth: 'token-2'}, connected, advance(10)],
  ),
  whileConnected(
    'Unauthorized moves to needs-auth; connect() without a token retries',
    {kind: 'Unauthorized', message: 'no', origin: 'server'},
    [{op: 'connect'}, connected],
  ),
  whileConnected(
    'PushFailed with HTTP 401 is a mutate auth failure',
    {
      kind: 'PushFailed',
      origin: 'zeroCache',
      reason: 'http',
      status: 401,
      bodyPreview: 'unauthorized',
      message: 'Fetch from API server returned non-OK status 401',
      mutationIDs: [{clientID: '$clientID', id: 1}],
    },
    [{op: 'connect', auth: 'token-2'}, connected],
  ),
  whileConnected('TransformFailed with HTTP 403 is a query auth failure', {
    kind: 'TransformFailed',
    origin: 'zeroCache',
    reason: 'http',
    status: 403,
    message: 'Fetch from API server returned non-OK status 403',
    queryIDs: ['q1'],
  }),
  whileConnected('PushFailed with HTTP 500 is an error', {
    kind: 'PushFailed',
    origin: 'zeroCache',
    reason: 'http',
    status: 500,
    message: 'Fetch from API server returned non-OK status 500',
    mutationIDs: [],
  }),
  whileConnected(
    'PushFailed from the server (database) is an error',
    {
      kind: 'PushFailed',
      origin: 'server',
      reason: 'database',
      message: 'db down',
      mutationIDs: [{clientID: '$clientID', id: 1}],
      details: {code: 1},
    },
    [{op: 'connect'}, connected],
  ),
  whileConnected('TransformFailed timeout is an error', {
    kind: 'TransformFailed',
    origin: 'zeroCache',
    reason: 'timeout',
    message: 'timed out',
    queryIDs: [],
  }),
  whileConnected(
    'ClientNotFound is an error and reports client state not found',
    {kind: 'ClientNotFound', message: 'no client', origin: 'zeroCache'},
    [{op: 'connect'}],
  ),
  whileConnected('VersionNotSupported reports an update is needed', {
    kind: 'VersionNotSupported',
    message: 'too old',
    origin: 'zeroCache',
  }),
  whileConnected('SchemaVersionNotSupported reports an update is needed', {
    kind: 'SchemaVersionNotSupported',
    message: 'schema too old',
    origin: 'zeroCache',
  }),
  whileConnected('InvalidConnectionRequest is an error', {
    kind: 'InvalidConnectionRequest',
    message: 'bad request',
    origin: 'zeroCache',
  }),
  whileConnected(
    'InvalidConnectionRequestBaseCookie drops the store and reports client state not found',
    {
      kind: 'InvalidConnectionRequestBaseCookie',
      message: 'cookie ahead',
      origin: 'zeroCache',
    },
  ),
  whileConnected(
    'InvalidConnectionRequestLastMutationID drops the store and reports client state not found',
    {
      kind: 'InvalidConnectionRequestLastMutationID',
      message: 'lmid ahead',
      origin: 'zeroCache',
    },
  ),
  whileConnected('InvalidConnectionRequestClientDeleted is an error', {
    kind: 'InvalidConnectionRequestClientDeleted',
    message: 'deleted',
    origin: 'zeroCache',
  }),
  whileConnected('InvalidMessage is an error', {
    kind: 'InvalidMessage',
    message: 'bad message',
    origin: 'zeroCache',
  }),
  whileConnected('InvalidPush is an error', {
    kind: 'InvalidPush',
    message: 'bad push',
    origin: 'zeroCache',
  }),
  whileConnected(
    'Internal is an error until connect()',
    {kind: 'Internal', message: 'boom', origin: 'zeroCache'},
    [{op: 'connect'}, connected, advance(10)],
  ),
  whileConnected('MutationFailed reconnects after the backoff', {
    kind: 'MutationFailed',
    message: 'failed',
    origin: 'zeroCache',
  }),
  {
    name: 'error while connected: MutationRateLimited keeps the connection and resends pending mutations',
    steps: [
      start,
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      mutate('m2', 'issue.create', {id: 'i2'}),
      errorMessage({
        kind: 'MutationRateLimited',
        message: 'slow down',
        origin: 'zeroCache',
      }),
      mutate('m3', 'issue.create', {id: 'i3'}),
      advance(5000),
    ],
  },
  {
    name: 'error while connected: Rebalance backs off at least minBackoffMs and adds reconnectParams once',
    steps: [
      start,
      connected,
      errorMessage({
        kind: 'Rebalance',
        message: 'moving',
        minBackoffMs: 8000,
        origin: 'zeroCache',
        reconnectParams: {reason: 'rebalance', shard: '2'},
      }),
      advance(7999),
      advance(1),
      {op: 'socketClose', code: 1006},
      advance(5000),
    ],
  },
  {
    name: 'error while connected: Rehome backs off at most maxBackoffMs',
    steps: [
      start,
      connected,
      errorMessage({
        kind: 'Rehome',
        message: 'rehomed',
        maxBackoffMs: 100,
        origin: 'zeroCache',
      }),
      advance(99),
      advance(1),
      connected,
    ],
  },
  {
    name: 'error while connected: ServerOverloaded with both bounds',
    steps: [
      start,
      connected,
      errorMessage({
        kind: 'ServerOverloaded',
        message: 'busy',
        origin: 'zeroCache',
        minBackoffMs: 2000,
        maxBackoffMs: 3000,
      }),
      advance(2999),
      advance(1),
    ],
  },
  {
    name: 'error while connecting: Unauthorized before connected',
    options: {auth: 'token-1'},
    steps: [
      start,
      errorMessage({
        kind: 'Unauthorized',
        message: 'bad token',
        origin: 'zeroCache',
      }),
      advance(10_000),
      {op: 'connect', auth: 'token-2'},
      connected,
    ],
  },
  {
    name: 'error while connecting: Rebalance before connected',
    steps: [
      start,
      errorMessage({
        kind: 'Rebalance',
        message: 'moving',
        reconnectParams: {to: 'b'},
        origin: 'zeroCache',
      }),
      advance(5000),
      connected,
    ],
  },
  {
    name: 'error while connecting: Internal before connected',
    steps: [
      start,
      errorMessage({kind: 'Internal', message: 'boom', origin: 'zeroCache'}),
      advance(10_000),
      {op: 'connect'},
      connected,
    ],
  },
  {
    name: 'error without an origin (older servers) is an internal error',
    steps: [
      start,
      connected,
      errorMessage({kind: 'Unauthorized', message: 'no origin'}),
      {op: 'connect'},
      connected,
    ],
  },
  {
    name: 'error without an origin still reports an update is needed',
    steps: [
      start,
      connected,
      errorMessage({kind: 'VersionNotSupported', message: 'too old'}),
    ],
  },
  {
    name: 'auth: connect({auth}) while connected sends updateAuth',
    options: {auth: 'token-1'},
    steps: [start, connected, {op: 'connect', auth: 'token-2'}, advance(10)],
  },
  {
    name: 'auth: connect({auth}) while connecting uses the token on the next socket',
    options: {auth: 'token-1'},
    steps: [
      start,
      {op: 'connect', auth: 'token-2'},
      {op: 'socketClose', code: 1006},
      advance(5000),
      connected,
    ],
  },
  {
    name: 'auth: a needs-auth error mid-poke drops the partial poke',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.all'},
      connected,
      {op: 'receive', message: ['pokeStart', {pokeID: 'p1', baseCookie: null}]},
      errorMessage({
        kind: 'AuthInvalidated',
        message: 'expired',
        origin: 'zeroCache',
      }),
      {op: 'connect', auth: 'token-2'},
      connected,
      {op: 'receive', message: ['pokeEnd', {pokeID: 'p1', cookie: '01'}]},
    ],
  },
  {
    name: 'error: an unparseable message is an error',
    steps: [
      start,
      connected,
      {op: 'receive', message: ['nonsense', {}]},
      {op: 'connect'},
      connected,
    ],
  },
];
