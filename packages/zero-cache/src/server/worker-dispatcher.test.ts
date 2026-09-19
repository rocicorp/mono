import type {SendHandle} from 'node:child_process';
import EventEmitter from 'node:events';
import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {encodeSecProtocols} from '../../../zero-protocol/src/connect.ts';
import {
  type ClientGroupStatusMessage,
  inProcChannel,
} from '../types/processes.ts';
import {parsePath, WorkerDispatcher} from './worker-dispatcher.ts';

test.each([
  ['/sync/v1/connect', {version: '1', worker: 'sync', action: 'connect'}],
  ['/sync/v2/connect', {version: '2', worker: 'sync', action: 'connect'}],
  [
    '/sync/v3/connect?foo=bar',
    {version: '3', worker: 'sync', action: 'connect'},
  ],
  [
    '/api/sync/v1/connect',
    {base: 'api', worker: 'sync', version: '1', action: 'connect'},
  ],
  [
    '/api/sync/v1/connect?a=b&c=d',
    {base: 'api', worker: 'sync', version: '1', action: 'connect'},
  ],
  [
    '/zero/sync/v1/connect',
    {base: 'zero', worker: 'sync', version: '1', action: 'connect'},
  ],
  [
    '/zero-api/sync/v0/connect',
    {base: 'zero-api', worker: 'sync', version: '0', action: 'connect'},
  ],
  [
    '/zero-api/sync/v2/connect?',
    {base: 'zero-api', worker: 'sync', version: '2', action: 'connect'},
  ],

  ['/mutate/v1/connect', {version: '1', worker: 'mutate', action: 'connect'}],
  ['/mutate/v2/connect', {version: '2', worker: 'mutate', action: 'connect'}],
  [
    '/mutate/v3/connect?foo=bar',
    {version: '3', worker: 'mutate', action: 'connect'},
  ],
  [
    '/api/mutate/v1/connect',
    {base: 'api', worker: 'mutate', version: '1', action: 'connect'},
  ],
  [
    '/api/mutate/v1/connect?a=b&c=d',
    {base: 'api', worker: 'mutate', version: '1', action: 'connect'},
  ],
  [
    '/zero/mutate/v1/connect',
    {base: 'zero', worker: 'mutate', version: '1', action: 'connect'},
  ],
  [
    '/zero-api/mutate/v0/connect',
    {base: 'zero-api', worker: 'mutate', version: '0', action: 'connect'},
  ],
  [
    '/zero-api/mutate/v2/connect?',
    {base: 'zero-api', worker: 'mutate', version: '2', action: 'connect'},
  ],
  [
    '/replication/v1/changes',
    {version: '1', worker: 'replication', action: 'changes'},
  ],
  [
    '/replication/v2/changes',
    {version: '2', worker: 'replication', action: 'changes'},
  ],
  [
    '/replication/v3/changes?foo=bar',
    {version: '3', worker: 'replication', action: 'changes'},
  ],
  [
    '/replication/v3/snapshot?id=foobar',
    {version: '3', worker: 'replication', action: 'snapshot'},
  ],
  [
    '/api/replication/v1/changes',
    {base: 'api', worker: 'replication', version: '1', action: 'changes'},
  ],
  [
    '/api/replication/v1/changes?a=b&c=d',
    {base: 'api', worker: 'replication', version: '1', action: 'changes'},
  ],

  ['/zero-api/sync/v2/connect/not/match', undefined],
  ['/too/many/components/sync/v0/connect', undefined],
  ['/random/path', undefined],
  ['/', undefined],
  ['', undefined],
])('parseSyncPath %s', (path, result) => {
  expect(parsePath(new URL(path, 'http://foo/'))).toEqual(result);
});

describe('WorkerDispatcher client group routing', () => {
  test('routes connections to least-loaded syncers and updates on clientGroupStatus', () => {
    const [parentInDispatcher, parentOut] = inProcChannel();
    const [syncer0InDispatcher, syncer0Out] = inProcChannel();
    const [syncer1InDispatcher, syncer1Out] = inProcChannel();

    const syncer0Messages: unknown[] = [];
    const syncer1Messages: unknown[] = [];
    syncer0Out.on('message', data => syncer0Messages.push(data));
    syncer1Out.on('message', data => syncer1Messages.push(data));

    const dispatcher = new WorkerDispatcher(
      createSilentLogContext(),
      'task-test',
      parentInDispatcher,
      [syncer0InDispatcher, syncer1InDispatcher],
      undefined,
      undefined,
    );

    const secProtocol = encodeSecProtocols(undefined, undefined);

    const sendSyncHandoff = (cg: string) => {
      const socket = new EventEmitter();
      parentOut.send(
        [
          'handoff',
          {
            message: {
              url: `/sync/v1/connect?clientID=c1&clientGroupID=${cg}&ts=100&lmid=1`,
              headers: {
                'sec-websocket-protocol': secProtocol,
              },
            },
            head: new ArrayBuffer(0),
          },
        ],
        socket as unknown as SendHandle,
      );
    };

    // First connection goes to one syncer
    sendSyncHandoff('cg-1');
    const assignedSyncer1 = syncer0Messages.length === 1 ? 0 : 1;
    expect(syncer0Messages.length + syncer1Messages.length).toBe(1);

    // Second connection for a DIFFERENT client group goes to the other syncer (least-loaded)
    sendSyncHandoff('cg-2');
    expect(syncer0Messages.length).toBe(1);
    expect(syncer1Messages.length).toBe(1);

    // Reconnecting cg-1 is sticky
    const s0Count = syncer0Messages.length;
    const s1Count = syncer1Messages.length;
    sendSyncHandoff('cg-1');
    if (assignedSyncer1 === 0) {
      expect(syncer0Messages.length).toBe(s0Count + 1);
      expect(syncer1Messages.length).toBe(s1Count);
    } else {
      expect(syncer0Messages.length).toBe(s0Count);
      expect(syncer1Messages.length).toBe(s1Count + 1);
    }

    // Confirm cg-1 and cg-2
    if (assignedSyncer1 === 0) {
      syncer0Out.send<ClientGroupStatusMessage>([
        'clientGroupStatus',
        {clientGroupID: 'cg-1', active: true},
      ]);
      syncer1Out.send<ClientGroupStatusMessage>([
        'clientGroupStatus',
        {clientGroupID: 'cg-2', active: true},
      ]);
    } else {
      syncer1Out.send<ClientGroupStatusMessage>([
        'clientGroupStatus',
        {clientGroupID: 'cg-1', active: true},
      ]);
      syncer0Out.send<ClientGroupStatusMessage>([
        'clientGroupStatus',
        {clientGroupID: 'cg-2', active: true},
      ]);
    }

    // Release cg-1; now assignedSyncer1 has load 0 and the other has load 1
    if (assignedSyncer1 === 0) {
      syncer0Out.send<ClientGroupStatusMessage>([
        'clientGroupStatus',
        {clientGroupID: 'cg-1', active: false},
      ]);
    } else {
      syncer1Out.send<ClientGroupStatusMessage>([
        'clientGroupStatus',
        {clientGroupID: 'cg-1', active: false},
      ]);
    }

    // Next new group must route to assignedSyncer1 (which has load 0)
    const s0Before = syncer0Messages.length;
    const s1Before = syncer1Messages.length;
    sendSyncHandoff('cg-3');
    if (assignedSyncer1 === 0) {
      expect(syncer0Messages.length).toBe(s0Before + 1);
      expect(syncer1Messages.length).toBe(s1Before);
    } else {
      expect(syncer0Messages.length).toBe(s0Before);
      expect(syncer1Messages.length).toBe(s1Before + 1);
    }

    void dispatcher.stop();
  });
});
