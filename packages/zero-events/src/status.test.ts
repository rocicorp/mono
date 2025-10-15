import {expectTypeOf, test} from 'vitest';
import type {ReplicationStatusEvent, StatusEvent} from './status.ts';

test('type name prefix required by StatusEvent', () => {
  expectTypeOf<
    StatusEvent & {type: 'zero/events/status/foo/bar'}
  >().toExtend<StatusEvent>();

  expectTypeOf<
    StatusEvent & {type: 'not/a/proper/subtype'}
  >().not.toExtend<StatusEvent>();
});

test('replication status event', () => {
  expectTypeOf<ReplicationStatusEvent>().toExtend<StatusEvent>();
});
