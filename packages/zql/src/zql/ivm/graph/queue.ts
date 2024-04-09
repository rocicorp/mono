import type {Multiset} from '../multiset.js';
import type {Version} from '../types.js';
import type {Reply} from './message.js';

export type QueueEntry<T> =
  | readonly [version: Version, multiset: Multiset<T>, reply: Reply]
  | readonly [version: Version, multiset: Multiset<T>];
