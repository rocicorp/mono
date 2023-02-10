// Generates the total merged ordering of mutation from the pending lists in
// [[clients]]. I don't know how to describe this total order more succinctly
// than:
// - each client maintains a list of pending mutations sorted by lmid

import type { ClientMutation } from "../types/client-mutation.js";
import type { ClientMap } from "../types/client-state.js";
import { PeekIterator } from "../util/peek-iterator.js";

// - we merge sort those lists, but the merge function is the server timestamp
export function* generateMergedMutations(clients: ClientMap) {
  // Build a list of mutation iterators sorted by next val's timestamp
  const iterators: PeekIterator<ClientMutation>[] = [];

  const insertIterator = (ins: PeekIterator<ClientMutation>) => {
    const { value, done } = ins.peek();
    if (done) {
      return;
    }
    const pos = iterators.findIndex(
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      (it) => it.peek().value!.timestamp > value!.timestamp
    );
    iterators.splice(pos === -1 ? iterators.length : pos, 0, ins);
  };

  for (const [clientID, c] of clients) {
    const clientMutations = c.pending.map((m) => ({ clientID, ...m }));
    insertIterator(new PeekIterator(clientMutations.values()));
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
    const { value, done } = next.peek();
    if (done) {
      throw new Error("unexpected state");
    }
    yield value as ClientMutation;
    next.next();
    insertIterator(next);
    //dumpIterators("after insert");
  }
}
