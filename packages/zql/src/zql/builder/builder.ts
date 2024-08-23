import {assert, unreachable} from 'shared/src/asserts.js';
import {AST} from '../ast2/ast.js';
import {Filter} from '../ivm2/filter.js';
import {Join} from '../ivm2/join.js';
import {Operator, Storage} from '../ivm2/operator.js';
import {Source} from '../ivm2/source.js';
import {createPredicate} from './filter.js';

/**
 * Interface required of caller to buildPipeline. Connects to constructed
 * pipeline to host environment to provide sources and storage.
 */
export interface Host {
  /**
   * Called once for each source needed by the AST.
   * Might be called multiple times with same tableName. It is OK to return
   * same storage instance in that case.
   */
  getSource(tableName: string): Source;

  /**
   * Called once for each operator that requires storage. Should return a new
   * unique storage object for each call.
   */
  createStorage(): Storage;
}

/**
 * Builds a pipeline from an AST. Caller must provide a Host to create source
 * and storage interfaces as necessary.
 *
 * Usage:
 *
 * ```ts
 * class MySink implements Output {
 *   readonly #input: Input;
 *
 *   constructor(input: Input) {
 *     this.#input = input;
 *     this.#input.setOutput(this);
 *     console.log([...this.#input.hydrate()]);
 *   }
 *
 *   push(change: Change, _: Operator) {
 *     console.log(change);
 *   }
 * }
 *
 * const input = buildPipeline(ast, myHost);
 * const sink = new MySink(input);
 * ```
 */
export function buildPipeline(ast: AST, host: Host) {
  const source = host.getSource(ast.table);
  let end: Operator = source.connect(ast.orderBy);

  if (ast.where) {
    end = new Filter(end, createPredicate(ast.where));
  }

  if (ast.limit) {
    // Limit not implemented yet.
    unreachable();
  }

  if (ast.subqueries) {
    for (const sq of ast.subqueries) {
      assert(sq.subquery.alias, 'Subquery must have an alias');
      const child = buildPipeline(sq.subquery, host);
      end = new Join(
        end,
        child,
        host.createStorage(),
        sq.correlation.parentField,
        sq.correlation.childField,
        sq.subquery.alias,
      );
    }
  }

  return end;
}
