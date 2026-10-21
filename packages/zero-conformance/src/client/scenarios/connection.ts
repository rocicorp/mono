/**
 * The connection lifecycle: the connect URL and handshake, ping and pong,
 * timeouts, closes, backoff, the disconnect timeout, and close().
 */
import type {Scenario} from '../harness.ts';
import {
  advance,
  connected,
  issue,
  mutate,
  poke,
  pong,
  put,
  start,
  queryHash,
  syncPoke,
} from './helpers.ts';

const openHash = queryHash('issues.open');

export const connectionScenarios: Scenario[] = [
  {
    name: 'connect: URL, auth, and initConnection in the sec protocol',
    options: {auth: 'token-1'},
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.open'},
      connected,
      advance(10),
    ],
  },
  {
    name: 'connect: queries registered before the socket opens are in initConnection',
    steps: [
      {op: 'materialize', view: 'a', query: 'issues.all'},
      {op: 'materialize', view: 'b', query: 'issues.byOwner', args: 'u2'},
      connected,
      advance(10),
    ],
  },
  {
    name: 'connect: queries registered while connecting are sent after connected',
    steps: [
      {op: 'materialize', view: 'a', query: 'issues.all'},
      {op: 'materialize', view: 'b', query: 'users.all'},
      connected,
      {op: 'destroy', view: 'a'},
      advance(10),
    ],
  },
  {
    name: 'connect: a logged-out client omits userID',
    options: {userID: null},
    steps: [start, connected],
  },
  {
    name: 'connect: mutate and query URLs and headers in initConnection',
    options: {
      mutateURL: 'https://api.test/mutate',
      mutateHeaders: {'x-mutate': '1'},
      queryURL: 'https://api.test/query',
      queryHeaders: {'x-query': '2'},
    },
    steps: [start, connected],
  },
  {
    name: 'connect: initConnection too large for the header is sent after connected',
    options: {maxHeaderLength: 200, auth: 'token-1'},
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.open'},
      connected,
      advance(10),
    ],
  },
  {
    name: 'connect: no cacheURL opens no socket and local mutations still apply',
    options: {cacheURL: null},
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.all'},
      mutate('m1', 'issue.create', {id: 'i1', title: 'local'}),
      advance(70_000),
      {op: 'connect'},
    ],
  },
  {
    name: 'ping: sent after pingTimeoutMs idle, and again after each pong',
    steps: [
      start,
      connected,
      advance(4999),
      advance(1),
      pong,
      advance(5000),
      pong,
    ],
  },
  {
    name: 'ping: a custom pingTimeoutMs',
    options: {pingTimeoutMs: 1000},
    steps: [start, connected, advance(1000), pong, advance(999), advance(1)],
  },
  {
    name: 'ping: no pong reconnects after the backoff',
    steps: [
      start,
      connected,
      advance(5000),
      advance(4999),
      advance(1),
      advance(4999),
      advance(1),
      connected,
    ],
  },
  {
    name: 'ping: poke frames restart the idle timer',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.open'},
      connected,
      advance(3000),
      ...syncPoke('p1', null, '01', [openHash]),
      advance(3000),
      advance(2000),
      advance(1),
    ],
  },
  {
    name: 'ping: a pushResponse does not restart the idle timer',
    steps: [
      start,
      connected,
      advance(3000),
      {
        op: 'receive',
        message: ['pushResponse', {mutations: []}],
      },
      advance(2000),
    ],
  },
  {
    name: 'timeout: no connected message within 10s closes and retries after the backoff',
    steps: [advance(9999), advance(1), advance(4999), advance(1), connected],
  },
  {
    name: 'close: abrupt close while connected reconnects at once',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.open'},
      connected,
      ...syncPoke('p1', null, '01', [openHash], [put('issues', issue('i1'))]),
      {op: 'socketClose', code: 1006},
      connected,
      advance(10),
    ],
  },
  {
    name: 'close: clean close while connected reconnects at once',
    steps: [
      start,
      connected,
      {op: 'socketClose', code: 1000, reason: 'bye', wasClean: true},
      connected,
    ],
  },
  {
    name: 'close: close while connecting retries after the backoff',
    steps: [
      start,
      {op: 'socketClose', code: 1006},
      advance(4999),
      advance(1),
      connected,
    ],
  },
  {
    name: 'close: an unexpected close code while connected',
    steps: [start, connected, {op: 'socketClose', code: 4000, reason: 'odd'}],
  },
  {
    name: 'close: message too big (1009) moves to error and reports client state not found',
    steps: [
      start,
      connected,
      {op: 'socketClose', code: 1009, wasClean: true},
      advance(10_000),
      mutate('m1', 'issue.create', {id: 'i1'}),
    ],
  },
  {
    name: 'close: messages from a socket the client closed are ignored',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.all'},
      connected,
      {op: 'socketClose', code: 1006},
      ...poke('p1', null, '01', {rowsPatch: [put('issues', issue('i1'))]}).map(
        step => ({...step, socket: 0}),
      ),
      {op: 'socketClose', code: 1006, socket: 0},
      {op: 'receive', message: ['connected', {wsid: 'w1'}], socket: 0},
      connected,
    ],
  },
  {
    name: 'disconnect timeout: never connecting moves to disconnected and keeps retrying',
    options: {disconnectTimeoutMs: 20_000},
    steps: [
      mutate('m1', 'issue.create', {id: 'i1'}),
      advance(10_000),
      advance(5000),
      advance(4000),
      advance(1000),
      mutate('m2', 'issue.create', {id: 'i2'}),
      advance(10_000),
      advance(5000),
      connected,
      mutate('m3', 'issue.create', {id: 'i3'}),
    ],
  },
  {
    name: 'disconnect timeout: the window restarts after a connection',
    options: {disconnectTimeoutMs: 12_000},
    steps: [
      advance(9000),
      connected,
      {op: 'socketClose', code: 1006},
      advance(10_000),
      advance(1999),
      advance(1),
    ],
  },
  {
    name: 'close(): closes the socket and settles pending mutations',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.all'},
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      {op: 'close'},
      mutate('m2', 'issue.create', {id: 'i2'}),
      advance(10_000),
      {op: 'connect'},
    ],
  },
  {
    name: 'close(): while connecting',
    steps: [{op: 'close'}, advance(20_000)],
  },
  {
    name: 'connect(): a no-op unless in a terminal state',
    steps: [start, {op: 'connect'}, connected, {op: 'connect'}, advance(10)],
  },
];
