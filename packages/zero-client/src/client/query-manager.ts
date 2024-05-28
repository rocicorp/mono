import {AST, normalizeAST} from '@rocicorp/zql/src/zql/ast/ast.js';
import type {ClientID} from 'replicache';
import type {ChangeDesiredQueriesMessage, QueriesPatch} from 'zero-protocol';
import type {ReadTransaction} from '../mod.js';
import xxh from 'xxhashjs';
import {GOT_QUERIES_KEY_PREFIX, desiredQueriesPrefixForClient} from './keys.js';
import type {ReplicacheImpl} from 'replicache/src/replicache-impl.js';
import {must} from 'shared/src/must.js';
import type {GotCallback} from '@rocicorp/zql/src/zql/context/context.js';

const defaultGotCallback = () => {};

export class QueryManager {
  readonly #clientID: ClientID;
  readonly #send: (change: ChangeDesiredQueriesMessage) => void;
  readonly #queries: Map<
    string,
    {normalized: AST; gotCallbacks: GotCallback[]}
  > = new Map();
  readonly #gotQueries: Set<string> = new Set();

  constructor(
    clientID: ClientID,
    send: (change: ChangeDesiredQueriesMessage) => void,
    experimentalWatch: InstanceType<typeof ReplicacheImpl>['experimentalWatch'],
  ) {
    this.#clientID = clientID;
    this.#send = send;
    experimentalWatch(
      diff => {
        for (const diffOp of diff) {
          const queryHash = diffOp.key.substring(GOT_QUERIES_KEY_PREFIX.length);
          switch (diffOp.op) {
            case 'add':
              this.#gotQueries.add(queryHash);
              this.#fireGotCallbacks(queryHash, true);
              break;
            case 'del':
              this.#gotQueries.delete(queryHash);
              this.#fireGotCallbacks(queryHash, false);
              break;
          }
        }
      },
      {
        prefix: GOT_QUERIES_KEY_PREFIX,
        initialValuesInFirstDiff: true,
      },
    );
  }

  #fireGotCallbacks(queryHash: string, got: boolean) {
    const gotCallbacks = this.#queries.get(queryHash)?.gotCallbacks ?? [];
    for (const gotCallback of gotCallbacks) {
      gotCallback(got);
    }
  }

  async getQueriesPatch(tx: ReadTransaction): Promise<QueriesPatch> {
    const existingQueryHashes = new Set<string>();
    const prefix = desiredQueriesPrefixForClient(this.#clientID);
    for await (const key of tx.scan({prefix}).keys()) {
      existingQueryHashes.add(key.substring(prefix.length, key.length));
    }
    const patch: QueriesPatch = [];
    for (const hash of existingQueryHashes) {
      if (!this.#queries.has(hash)) {
        patch.push({op: 'del', hash});
      }
    }
    for (const [hash, {normalized}] of this.#queries) {
      if (!existingQueryHashes.has(hash)) {
        patch.push({op: 'put', hash, ast: normalized});
      }
    }
    return patch;
  }

  add(ast: AST, gotCallback: GotCallback = defaultGotCallback): () => void {
    const normalized = normalizeAST(ast);
    const astHash = hash(normalized);
    let entry = this.#queries.get(astHash);
    if (!entry) {
      entry = {normalized, gotCallbacks: [gotCallback]};
      this.#queries.set(astHash, entry);
      this.#send([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'put', hash: astHash, ast: normalized}],
        },
      ]);
    } else {
      entry.gotCallbacks.push(gotCallback);
    }

    queueMicrotask(() => {
      gotCallback(this.#gotQueries.has(astHash));
    });
    let removed = false;
    return () => {
      if (removed) {
        return;
      }
      removed = true;
      this.#remove(astHash, gotCallback);
    };
  }

  #remove(astHash: string, gotCallback: GotCallback) {
    const entry = must(this.#queries.get(astHash));
    entry.gotCallbacks.splice(entry.gotCallbacks.indexOf(gotCallback), 1);
    if (entry.gotCallbacks.length === 0) {
      this.#queries.delete(astHash);
      this.#send([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'del', hash: astHash}],
        },
      ]);
    }
    return true;
  }
}

function hash(normalized: AST): string {
  return xxh.h64(0).update(JSON.stringify(normalized)).digest().toString(36);
}
