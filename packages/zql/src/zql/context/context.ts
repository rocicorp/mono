import {Materialite} from '../ivm/materialite.js';
import {Source} from '../ivm/source/source.js';
import {Entity} from '../../generate.js';
import {Ordering} from '../ast/ast.js';
import {compareUTF8} from 'compare-utf8';

/**
 * Used to integrate with the host environment.
 *
 * A source is a table or collection which ZQL can query.
 * The name of a source represents the name of the table
 * ZQL is querying.
 */
export type Context = {
  materialite: Materialite;
  getSource: <T extends Entity>(name: string, ordering?: Ordering) => Source<T>;
};

export function makeTestContext(): Context {
  const materialite = new Materialite();
  const sources = new Map<string, Source<object>>();
  const getSource = <T extends Entity>(name: string) => {
    if (!sources.has(name)) {
      sources.set(
        name,
        materialite.newSetSource<T>((l, r) =>
          compareUTF8(l.id, r.id),
        ) as unknown as Source<object>,
      );
    }
    return sources.get(name)! as unknown as Source<T>;
  };
  return {materialite, getSource};
}
