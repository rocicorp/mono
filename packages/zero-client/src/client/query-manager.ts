import {AST, normalizeAST} from '@rocicorp/zql/src/zql/ast/ast.js';
import type {ClientID} from 'replicache';
import type {ChangeDesiredQueriesMessage, QueriesPatch} from 'zero-protocol';
import type {ReadTransaction} from '../mod.js';
import xxh from 'xxhashjs';
import {desiredQueriesPrefixForClient} from './keys.js';

export class QueryManager {
  readonly #clientID: ClientID;
  readonly #send: (change: ChangeDesiredQueriesMessage) => void;
  readonly #queries: Map<string, {normalized: AST; refCount: number}> =
    new Map();

  constructor(
    clientID: ClientID,
    send: (change: ChangeDesiredQueriesMessage) => void,
  ) {
    this.#clientID = clientID;
    this.#send = send;
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

  add(ast: AST) {
    const normalized = normalizeAST(ast);
    const astHash = hash(normalized);
    const entry = this.#queries.get(astHash);
    if (!entry) {
      const newEntry = {normalized, refCount: 1};
      this.#queries.set(astHash, newEntry);
      this.#send([
        'changeDesiredQueries',
        {
          desiredQueriesPatch: [{op: 'put', hash: astHash, ast: normalized}],
        },
      ]);
      return;
    }
    entry.refCount++;
  }

  remove(ast: AST): boolean {
    const normalized = normalizeAST(ast);
    const astHash = hash(normalized);
    const entry = this.#queries.get(astHash);
    if (!entry) {
      return false;
    }
    entry.refCount--;
    if (entry.refCount === 0) {
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
