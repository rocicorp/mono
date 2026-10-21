/**
 * Visibility: the client disconnects once the app has been in the background
 * (a hidden tab) for `hiddenTabDisconnectDelay`, and reconnects when it comes
 * back. The Swift client reports its app's lifecycle the same way.
 */
import type {Scenario} from '../harness.ts';
import {
  advance,
  connected,
  issue,
  mutate,
  put,
  queryHash,
  start,
  syncPoke,
} from './helpers.ts';

const allHash = queryHash('issues.all');
const hidden = {op: 'visibility', state: 'hidden'} as const;
const visible = {op: 'visibility', state: 'visible'} as const;

export const visibilityScenarios: Scenario[] = [
  {
    name: 'visibility: a hidden app disconnects after the delay and reconnects when visible',
    options: {hiddenTabDisconnectDelay: 1000},
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
      connected,
      ...syncPoke('p1', null, '01', [allHash], [put('issues', issue('i1'))]),
      hidden,
      advance(999),
      advance(1),
      mutate('m1', 'issue.create', {id: 'i2'}),
      advance(10_000),
      visible,
      connected,
      ...syncPoke('p2', '01', '02', [allHash]),
    ],
  },
  {
    name: 'visibility: coming back before the delay keeps the connection',
    options: {hiddenTabDisconnectDelay: 1000},
    steps: [start, connected, hidden, advance(500), visible, advance(1000)],
  },
  {
    name: 'visibility: a hidden app does not reconnect after the socket drops',
    options: {hiddenTabDisconnectDelay: 0},
    steps: [
      start,
      connected,
      {op: 'socketClose', code: 1006},
      hidden,
      advance(5000),
      advance(60_000),
      visible,
      connected,
    ],
  },
  {
    name: 'visibility: hidden while connecting disconnects once connected',
    options: {hiddenTabDisconnectDelay: 0},
    steps: [start, hidden, connected, visible, connected],
  },
  {
    name: 'visibility: a pending mutation is pushed again after coming back',
    options: {hiddenTabDisconnectDelay: 0},
    steps: [
      start,
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      hidden,
      visible,
      connected,
    ],
  },
];
