import {describe, expect, test} from 'vitest';
import type {Sink} from '../../types/streams.ts';
import type {
  ChangeSourceUpstream,
  ChangeStreamMessage,
} from '../change-source/protocol/current.ts';
import {UpstreamAcker} from './upstream-acker.ts';

function sink(): {
  sink: Sink<ChangeSourceUpstream>;
  acked: ChangeSourceUpstream[];
} {
  const acked: ChangeSourceUpstream[] = [];
  return {sink: {push: msg => acked.push(msg)}, acked};
}

function commit(watermark: string): ChangeStreamMessage {
  return ['commit', {tag: 'commit'}, {watermark}];
}

function status(watermark: string, ack = true): ChangeStreamMessage {
  return ['status', {ack}, {watermark}];
}

describe('change-streamer/upstream-acker', () => {
  test('requires at least one store to be tracked', () => {
    expect(
      () => new UpstreamAcker({trackPgChangeLog: false, trackBackup: false}),
    ).toThrowErrorMatchingInlineSnapshot(
      `[Error: At least one of trackPgChangeLog or trackBackup must be true]`,
    );
  });

  test('acks only once the (sole) tracked pg change-log catches up', () => {
    const acker = new UpstreamAcker({
      trackPgChangeLog: true,
      trackBackup: false,
    });
    const {sink: upstream, acked} = sink();
    acker.reset(upstream);

    acker.trackDownstream(commit('05'));
    expect(acked).toEqual([]);

    acker.trackPgChangeLog('05');
    expect(acked).toEqual([['status', {tag: 'commit'}, {watermark: '05'}]]);
  });

  test('acks only once the (sole) tracked backup catches up', () => {
    const acker = new UpstreamAcker({
      trackPgChangeLog: false,
      trackBackup: true,
    });
    const {sink: upstream, acked} = sink();
    acker.reset(upstream);

    acker.trackDownstream(commit('05'));
    expect(acked).toEqual([]);

    acker.trackBackup('05');
    expect(acked).toEqual([['status', {tag: 'commit'}, {watermark: '05'}]]);
  });

  test('when tracking both stores, acks are gated by the slower of the two', () => {
    const acker = new UpstreamAcker({
      trackPgChangeLog: true,
      trackBackup: true,
    });
    const {sink: upstream, acked} = sink();
    acker.reset(upstream);

    acker.trackDownstream(commit('05'));

    // The pg change-log races ahead of the backup: nothing should be acked
    // yet, since the backup (the slower store) has not persisted '05'.
    acker.trackPgChangeLog('05');
    expect(acked).toEqual([]);

    // Once the backup (the slower store) catches up, the watermark is acked.
    acker.trackBackup('05');
    expect(acked).toEqual([['status', {tag: 'commit'}, {watermark: '05'}]]);
  });

  test('when tracking both stores, the ack watermark is the min of the two', () => {
    const acker = new UpstreamAcker({
      trackPgChangeLog: true,
      trackBackup: true,
    });
    const {sink: upstream, acked} = sink();
    acker.reset(upstream);

    acker.trackDownstream(commit('05'));
    acker.trackDownstream(commit('0a'));

    acker.trackBackup('0a'); // backup races ahead
    expect(acked).toEqual([]);

    acker.trackPgChangeLog('05'); // pg change-log catches up to '05' only
    expect(acked).toEqual([['status', {tag: 'commit'}, {watermark: '05'}]]);

    acker.trackPgChangeLog('0a'); // pg change-log catches up to '0a'
    expect(acked).toEqual([
      ['status', {tag: 'commit'}, {watermark: '05'}],
      ['status', {tag: 'commit'}, {watermark: '0a'}],
    ]);
  });

  test('does not re-ack a watermark that has already been acked', () => {
    const acker = new UpstreamAcker({
      trackPgChangeLog: true,
      trackBackup: false,
    });
    const {sink: upstream, acked} = sink();
    acker.reset(upstream);

    acker.trackDownstream(commit('05'));
    acker.trackPgChangeLog('05');
    acker.trackPgChangeLog('05'); // redundant notification
    expect(acked).toEqual([['status', {tag: 'commit'}, {watermark: '05'}]]);
  });

  test('acks a non-transactional status watermark once preceding commits are persisted', () => {
    const acker = new UpstreamAcker({
      trackPgChangeLog: true,
      trackBackup: false,
    });
    const {sink: upstream, acked} = sink();
    acker.reset(upstream);

    acker.trackDownstream(commit('05'));
    acker.trackDownstream(status('06'));
    // The status watermark should not be acked before '05' is persisted.
    expect(acked).toEqual([]);

    acker.trackPgChangeLog('05');
    expect(acked).toEqual([
      ['status', {tag: 'commit'}, {watermark: '05'}],
      ['status', {ack: true}, {watermark: '06'}],
    ]);
  });

  test('acks a status watermark immediately if there are no outstanding commits', () => {
    const acker = new UpstreamAcker({
      trackPgChangeLog: true,
      trackBackup: false,
    });
    const {sink: upstream, acked} = sink();
    acker.reset(upstream);

    acker.trackDownstream(status('06'));
    expect(acked).toEqual([['status', {ack: true}, {watermark: '06'}]]);
  });

  test('ignores status messages that do not request an ack', () => {
    const acker = new UpstreamAcker({
      trackPgChangeLog: true,
      trackBackup: false,
    });
    const {sink: upstream, acked} = sink();
    acker.reset(upstream);

    acker.trackDownstream(status('06', false));
    expect(acked).toEqual([]);
  });

  test('a replayed commit is acked immediately if already covered by a persisted watermark from before a reconnect', () => {
    const acker = new UpstreamAcker({
      trackPgChangeLog: true,
      trackBackup: false,
    });
    const {sink: upstream1, acked: acked1} = sink();
    acker.reset(upstream1);

    acker.trackDownstream(commit('0a'));
    acker.trackPgChangeLog('0a');
    expect(acked1).toEqual([['status', {tag: 'commit'}, {watermark: '0a'}]]);

    // Simulate a reconnect (e.g. a temporarily dropped replication slot)
    // where the change-source resumes from an earlier watermark ('05'),
    // replaying a commit that was already persisted before the disconnect.
    // This should be acked immediately -- from the store watermark alone,
    // without waiting for another trackPgChangeLog() call, which may never
    // come for a watermark that was already processed.
    const {sink: upstream2, acked: acked2} = sink();
    acker.reset(upstream2);
    acker.trackDownstream(commit('05'));
    expect(acked2).toEqual([['status', {tag: 'commit'}, {watermark: '0a'}]]);
  });

  test('reset() re-acks the current watermark to a new upstream sink', () => {
    const acker = new UpstreamAcker({
      trackPgChangeLog: true,
      trackBackup: false,
    });
    const {sink: upstream1, acked: acked1} = sink();
    acker.reset(upstream1);

    acker.trackDownstream(commit('05'));
    acker.trackPgChangeLog('05');
    expect(acked1).toEqual([['status', {tag: 'commit'}, {watermark: '05'}]]);

    // Simulate a reconnect: a new upstream sink should be re-informed of the
    // watermark once persisted, since the store watermarks (unlike the
    // per-connection tracking) survive the reset.
    const {sink: upstream2, acked: acked2} = sink();
    acker.reset(upstream2);
    expect(acked2).toEqual([]);

    acker.trackPgChangeLog('05');
    expect(acked2).toEqual([['status', {tag: 'commit'}, {watermark: '05'}]]);
  });
});
