import {compareUTF8} from 'compare-utf8';
import type {Entity} from '../../entity.js';
import type {AST} from '../ast/ast.js';
import {Materialite} from '../ivm/materialite.js';
import type {Source} from '../ivm/source/source.js';

export type SubscriptionDelegate = {
  subscriptionAdded(ast: AST): void;
  subscriptionRemoved(ast: AST): void;
};

/**
 * Used to integrate with the host environment.
 *
 * A source is a table or collection which ZQL can query.
 * The name of a source represents the name of the table
 * ZQL is querying.
 */
export type Context = SubscriptionDelegate & {
  materialite: Materialite;
  getSource: <T extends Entity>(name: string) => Source<T>;
};

class TestContext implements Context {
  readonly materialite = new Materialite();
  readonly #sources = new Map<string, Source<object>>();

  getSource<T extends Entity>(name: string): Source<T> {
    if (!this.#sources.has(name)) {
      const source = this.materialite.newSetSource((l: T, r: T) =>
        compareUTF8(l.id, r.id),
      ) as unknown as Source<object>;
      source.seed([]);
      this.#sources.set(name, source);
    }
    return this.#sources.get(name)! as unknown as Source<T>;
  }

  subscriptionAdded(_ast: AST): void {
    // Implement the subscriptionAdded method here
  }

  subscriptionRemoved(_ast: AST): void {
    // Implement the subscriptionRemoved method here
  }
}

export function makeTestContext(): Context {
  return new TestContext();
}
