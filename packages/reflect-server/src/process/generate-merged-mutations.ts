// Generates the total merged ordering of mutation from the pending lists in
// [[clients]] and [[clientGroups]].

import type {PendingMutationMap} from '../types/mutation.js';
import type {Mutation} from '../protocol/push.js';
import {PeekIterator} from '../util/peek-iterator.js';

// - we merge sort those lists, but the merge function is the server timestamp
export function* generateMergedMutations(pendingMutations: PendingMutationMap) {
  // Build a list of mutation iterators sorted by next val's timestamp
  const iterators: PeekIterator<Mutation>[] = [];

  const insertIterator = (ins: PeekIterator<Mutation>) => {
    const {value, done} = ins.peek();
    if (done) {
      return;
    }
    const pos = iterators.findIndex(
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      it => it.peek().value!.timestamp > value!.timestamp,
    );
    iterators.splice(pos === -1 ? iterators.length : pos, 0, ins);
  };

  for (const mutations of pendingMutations.values()) {
    insertIterator(new PeekIterator(mutations.values()));
  }

  // const dumpIterators = (msg: string) => {
  //   console.log(`iterators - ${msg}`);
  //   for (const it of iterators) {
  //     console.log(it.peek());
  //   }
  // };

  //dumpIterators("start");

  for (;;) {
    const next = iterators.shift();
    //dumpIterators("after shift");
    if (!next) {
      break;
    }
    const {value, done} = next.peek();
    if (done) {
      throw new Error('unexpected state');
    }
    yield value;
    next.next();
    insertIterator(next);
    //dumpIterators("after insert");
  }
}
