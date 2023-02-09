import type {ClientID} from '../types/client-state.js';
import type {Mutation} from '../protocol/push.js';
import type {ClientMutation} from '../types/client-mutation.js';

export class TurnBuffer {
  private readonly _turns: ClientMutation[][];

  constructor() {
    this._turns = [[], [], []];
  }

  // insert mutations with following constraints
  // - a client's mutation ids must be in ascending order with no gaps
  // - order of mutations is maintained
  // consider doing a validation pass, then an actual insert pass
  addMutations(
    mutations: Mutation[],
    clientID: ClientID,
    pushUnixTimestamp: number,
  ) {
    console.log(JSON.stringify(mutations));
    console.log(JSON.stringify(this._turns));
    if (mutations.length === 0) {
      return;
    }
    let lastMutationID = -1;
    let tOfLastMutation = -1;
    let mInTOfLastMutation = -1;
    for (let t = this._turns.length - 1; t >= 0 && lastMutationID === -1; t--) {
      const turn = this._turns[t];
      for (let m = turn.length - 1; m >= 0 && lastMutationID === -1; m--) {
        const cMutation = turn[m];
        if (cMutation.clientID === clientID) {
          lastMutationID = cMutation.id;
          tOfLastMutation = t;
          mInTOfLastMutation = m;
        }
      }
    }

    let minExistingUnixTimestamp = -1;
    for (const turn of this._turns) {
      for (const cMutation of turn) {
        if (!cMutation.old) {
          minExistingUnixTimestamp = cMutation.unixTimestamp;
          break;
        }
      }
    }

    for (const mutation of mutations) {
      if (mutation.id <= lastMutationID) {
        continue;
      }
      if (lastMutationID !== -1 && mutation.id !== lastMutationID + 1) {
        throw new Error(
          `unexpected mutation id ${clientID}, ${mutation.id}, ${lastMutationID}`,
        );
      }
      const old =
        pushUnixTimestamp - mutation.unixTimestamp > 50 &&
        (minExistingUnixTimestamp === -1 ||
          mutation.unixTimestamp <= minExistingUnixTimestamp);
      console.log(
        JSON.stringify(mutation),
        old,
        pushUnixTimestamp,
        mutation.unixTimestamp,
        pushUnixTimestamp - mutation.unixTimestamp,
        mutation.unixTimestamp,
        minExistingUnixTimestamp,
      );
      let inserted = false;
      for (
        let t = tOfLastMutation === -1 ? 0 : tOfLastMutation;
        t < this._turns.length && !inserted;
        t++
      ) {
        const turn = this._turns[t];
        for (
          let m = t === tOfLastMutation ? mInTOfLastMutation + 1 : 0;
          m < turn.length && !inserted;
          m++
        ) {
          const cMutation = turn[m];
          if (old || cMutation.unixTimestamp > mutation.unixTimestamp) {
            // insert
            turn.splice(m, 0, {...mutation, clientID, old});
            lastMutationID = mutation.id;
            tOfLastMutation = t;
            mInTOfLastMutation = m;
            inserted = true;
          }
        }
      }
      if (!inserted) {
        const lastTurn = this._turns[this._turns.length - 1];
        lastTurn.push({...mutation, clientID, old});
        lastMutationID = mutation.id;
        tOfLastMutation = this._turns.length - 1;
        mInTOfLastMutation = lastTurn.length - 1;
      }
    }
  }

  dequeueTurn(): ClientMutation[] {
    console.log('dequeueTurn', JSON.stringify(this._turns));
    const turn = this._turns.shift();
    this._turns.push([]);
    return turn ?? [];
  }

  isEmpty(): boolean {
    console.log('isEmpty', JSON.stringify(this._turns));
    for (const turn of this._turns) {
      if (turn.length > 0) {
        return false;
      }
    }
    return true;
  }
}

// Start of logic for DD31
// const newTurns = Array.from(this._turns, turn => [...turn]);
// let lastAddedT = -1;
// let lastAddedMInT = -1;
// const;
// for (const mutation of mutations) {
//   let minT = 0;
//   let minMInT = 0;
//   for (let t = newTurns.length - 1; t >= lastAddedT; t--) {
//     const turn = newTurns[t];
//     for (
//       let m = turn.length - 1;
//       m >= (t === lastAddedT ? lastAddedMInT : 0);
//       m--
//     ) {
//       const tMutation = turn[m];
//       if (tMutation.clientID === mutation.clientID) {
//         if (mutation.id < tMutation.id) {
//           // already queued
//         }
//         if (tMutation.id !== mutation.id - 1) {
//           throw new Error();
//         }
//         minT = t;
//         minMInT = m + 1;
//       }
//     }
//     for (let t = minT; t < newTurns.length; t++) {
//       const turn = newTurns[t];
//       for (let m = t === minT ? minMInT : 0; m < turn.length; m++) {
//         const tMutation = turn[m];
//         if (tMutation.unixTimestamp > mutation.unixTimestamp) {
//           turn.splice(m, 0, mutation);
//           lastAddedT = t;
//           lastAddedMInT = m;
//         }
//       }
//     }
//   }
