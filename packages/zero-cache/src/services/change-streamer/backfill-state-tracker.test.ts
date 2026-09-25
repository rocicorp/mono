import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {
  BackfillProgressMark,
  BackfillRequest,
  ChangeStreamData,
  MessageBackfill,
} from '../change-source/protocol/current.ts';
import {BackfillStateTracker} from './backfill-state-tracker.ts';
import type {SubscriberContext} from './change-streamer.ts';
import {createSubscriber} from './test-utils.ts';

function mark(progressMark: string, timeline = 't1'): BackfillProgressMark {
  return {progressMark, timeline};
}

const FOO = {schema: 'public', name: 'foo'};
const ROW_KEY = {columns: ['id'], type: 'default' as const};

function request(
  columns: Record<string, BackfillProgressMark | undefined>,
): BackfillRequest {
  return {
    table: {...FOO, metadata: null},
    columns: Object.fromEntries(
      Object.entries(columns).map(([col, progress]) => [
        col,
        progress ? {id: {col}, progress} : {id: {col}},
      ]),
    ),
  };
}

function backfill(
  columns: string[],
  progressMarks: MessageBackfill['progressMarks'],
): ChangeStreamData {
  return [
    'data',
    {
      tag: 'backfill',
      relation: {...FOO, rowKey: ROW_KEY},
      columns,
      watermark: '01',
      rowValues: [],
      progressMarks,
    },
  ];
}

const BEGIN: ChangeStreamData = [
  'begin',
  {tag: 'begin'},
  {commitWatermark: '02'},
];
const COMMIT: ChangeStreamData = ['commit', {tag: 'commit'}, {watermark: '02'}];

function ctx(backfills: BackfillRequest[] | undefined): SubscriberContext {
  return {
    protocolVersion: 8,
    taskID: 'task',
    id: 'sub',
    mode: 'serving',
    replicaVersion: 'rv1',
    watermark: '00',
    backfills,
  };
}

describe('change-streamer/backfill-state-tracker', () => {
  test('startStream returns the superset of the log and aligned subscribers', () => {
    const lc = createSilentLogContext();
    const tracker = new BackfillStateTracker(lc);

    // Seed the log's tracked column (from the cookie jar, which carries no
    // progress) and advance its in-memory progress for `a` to '05'.
    tracker.startStream([request({a: undefined})]);
    tracker.track(BEGIN);
    tracker.track(backfill(['a'], {current: mark('05')}));
    tracker.track(COMMIT);

    // An aligned subscriber pending on an earlier mark for `a`.
    const options = tracker.subscriberOptions(
      lc,
      ctx([request({a: mark('03')})]),
    );
    const [sub, , downstream] = createSubscriber('00', false, options);
    tracker.register(sub, downstream);
    void sub.setCaughtUp();

    // Restarting the stream: the cookie jar still reports no progress, but
    // the tracker carries over the log's in-memory progress ('05'), and the
    // superset with the subscriber's ('03') takes the earlier of the two.
    const requests = tracker.startStream([request({a: undefined})]);
    expect(requests).toEqual([request({a: mark('03')})]);
  });

  test('startStream ignores unaligned and v7 (no backfills) subscribers', () => {
    const lc = createSilentLogContext();
    const tracker = new BackfillStateTracker(lc);

    // v7 subscriber: reports no backfills, so subscriberOptions is a no-op.
    const v7Options = tracker.subscriberOptions(lc, ctx(undefined));
    expect(v7Options).toEqual({});
    const [v7Sub, , v7Downstream] = createSubscriber('00', true, v7Options);
    tracker.register(v7Sub, v7Downstream);

    // v8 subscriber that never catches up (not aligned).
    const options = tracker.subscriberOptions(
      lc,
      ctx([request({a: mark('09')})]),
    );
    const [sub, , downstream] = createSubscriber('00', false, options);
    tracker.register(sub, downstream);
    // Not calling sub.setCaughtUp(): the subscriber never aligns.

    const requests = tracker.startStream([request({a: undefined})]);

    // Neither the v7 nor the unaligned subscriber contributes to the merge.
    expect(requests).toEqual([request({a: undefined})]);
  });

  test('an uncovered aligned subscriber sets a restart reason', () => {
    const lc = createSilentLogContext();
    const tracker = new BackfillStateTracker(lc);

    // Start a session and advance it to `a: mark('05')`.
    tracker.startStream([request({a: undefined})]);
    tracker.track(BEGIN);
    tracker.track(backfill(['a'], {current: mark('05')}));
    tracker.track(COMMIT);
    expect(tracker.restartReason).toBeNull();

    // A subscriber pending on an earlier mark ('02') is not covered: the
    // session has already moved past the point it needs to resume from.
    const options = tracker.subscriberOptions(
      lc,
      ctx([request({a: mark('02')})]),
    );
    const [sub, , downstream] = createSubscriber('00', false, options);
    tracker.register(sub, downstream);

    expect(tracker.restartReason).toBeNull();
    void sub.setCaughtUp(); // triggers onAligned -> #checkCoverage
    expect(tracker.restartReason).not.toBeNull();
    expect(tracker.restartReason).toEqual(expect.stringContaining(sub.id));
    expect(tracker.restartReason).toEqual(
      expect.stringContaining('public.foo.a'),
    );
  });

  test('a covered aligned subscriber sets no restart reason', () => {
    const lc = createSilentLogContext();
    const tracker = new BackfillStateTracker(lc);

    // Start a session and advance it to `a: mark('05')`.
    tracker.startStream([request({a: undefined})]);
    tracker.track(BEGIN);
    tracker.track(backfill(['a'], {current: mark('05')}));
    tracker.track(COMMIT);

    // A subscriber pending on a later-or-equal mark ('08') is covered: the
    // session already delivered a continuous chain up to it.
    const options = tracker.subscriberOptions(
      lc,
      ctx([request({a: mark('08')})]),
    );
    const [sub, , downstream] = createSubscriber('00', false, options);
    tracker.register(sub, downstream);

    void sub.setCaughtUp();
    expect(tracker.restartReason).toBeNull();
  });

  test('an unregistered subscriber does not trigger a coverage check', () => {
    const lc = createSilentLogContext();
    const tracker = new BackfillStateTracker(lc);

    tracker.startStream([request({a: undefined})]);
    tracker.track(BEGIN);
    tracker.track(backfill(['a'], {current: mark('05')}));
    tracker.track(COMMIT);

    const options = tracker.subscriberOptions(
      lc,
      ctx([request({a: mark('02')})]),
    );
    // Note: no register() call.
    const [sub] = createSubscriber('00', false, options);

    void sub.setCaughtUp();
    expect(tracker.restartReason).toBeNull();
  });
});
