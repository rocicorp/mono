import type {ZeroEvent} from './index.ts';
import type {StatusEvent} from './status.ts';
import type {Extend} from './util.ts';

type IsValid<T> = [T] extends [never] ? false : true;

export const extendAcceptsValidType: IsValid<
  Extend<StatusEvent, {type: 'zero/events/status/foo/bar'}>
> = true;

export const extendRejectsInvalidType: IsValid<
  Extend<StatusEvent, {type: 'not/a/proper/subtype'}>
> = false;

export const extendAllowsNarrowing: IsValid<
  Extend<ZeroEvent, {type: 'foo/bar/baz'}>
> = true;

export const extendRejectsTypeChange: IsValid<
  Extend<ZeroEvent, {type: number}>
> = false;
