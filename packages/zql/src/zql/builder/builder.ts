import {assert, unreachable} from 'shared/src/asserts.js';
import {AST} from '../ast2/ast.js';
import {Filter} from '../ivm2/filter.js';
import {Join} from '../ivm2/join.js';
import {Input, Operator, Output, Storage} from '../ivm2/operator.js';
import {Source} from '../ivm2/source.js';
import {createPredicate} from './filter.js';

/**
 * Interface required of caller to buildPipeline. Connects to constructed
 * pipeline to host environment to provide sources, sinks, and storage.
 */
export interface Host<Sink extends Output> {
  /**
   * Called once for each source needed by the AST.
   * Might be called multiple times with same tableName. It is OK to return
   * same storage instance in that case.
   */
  getSource(tableName: string): Source;

  /**
   * Called once to create the final output for the pipeline.
   * Implementation should create `input` and can call `input.hydrate()` to
   * get initial query results. The pipeline will call `push()` on the
   * returned `Output` to notify caller of changes to the query results.
   */
  createSink(input: Input): Sink;

  /**
   * Called once for each operator that requires storage. Should return a new
   * unique storage object for each call.
   */
  createStorage(): Storage;
}

/**
 * Builds a pipeline from an AST. Caller must provide a Host to create sources,
 * the final Output (the "sink"), and storage interfaces as necessary.
 *
 * The return value is the same Sink instance created by `createSink()`, but
 * with that sink correctly installed in the pipeline.
 */
export function buildPipeline<Sink extends Output>(ast: AST, host: Host<Sink>) {
  const source = host.getSource(ast.table);
  let end: Operator = source.connect(ast.orderBy);

  if (ast.where) {
    const filter = new Filter(end, createPredicate(ast.where));
    end.setOutput(filter);
    end = filter;
  }

  if (ast.limit) {
    // Limit not implemented yet.
    unreachable();
  }

  if (ast.subqueries) {
    for (const sq of ast.subqueries) {
      const join = buildPipeline(sq.subquery, {
        ...host,
        createSink: childInput => {
          assert(sq.subquery.alias, 'Subquery must have an alias');
          return new Join(
            end,
            childInput,
            host.createStorage(),
            sq.correlation.parentField,
            sq.correlation.childField,
            sq.subquery.alias,
          );
        },
      });
      end.setOutput(join);
      end = join as Join;
    }
  }

  const sink = host.createSink(end);
  end.setOutput(sink);

  return sink;
}
