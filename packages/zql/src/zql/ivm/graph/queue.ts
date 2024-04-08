import {Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {Reply} from './message.js';

export type QueueEntry<T> =
  | readonly [version: Version, multiset: Multiset<T>, reply: Reply]
  | readonly [version: Version, multiset: Multiset<T>];
