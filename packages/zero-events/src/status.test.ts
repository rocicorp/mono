import {expectTypeOf, test} from 'vitest';
import type {ReplicationStatusEvent, StatusEvent} from './status.ts';
import type {Extend} from './util.ts';

test('type name prefix required by StatusEvent', () => {
  expectTypeOf<
    Extend<StatusEvent, {type: 'zero/events/status/foo/bar'}>
  >().not.toBeNever();

  expectTypeOf<
    Extend<StatusEvent, {type: 'not/a/proper/subtype'}>
  >().toBeNever();
});

test('replication status event', () => {
  expectTypeOf<ReplicationStatusEvent>().not.toBeNever();
  expectTypeOf<ReplicationStatusEvent>().toExtend<StatusEvent>();
});
