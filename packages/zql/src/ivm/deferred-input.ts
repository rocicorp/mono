import {assert} from '../../../shared/src/asserts.ts';
import {makeAddChange} from './change.ts';
import type {Node} from './data.ts';
import type {FetchRequest, Input, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {consume, type Stream} from './stream.ts';

/**
 * A placeholder `Input` for a view whose pipeline cannot be built yet.
 *
 * Until {@link attach} is called it behaves like an empty pipeline: `fetch`
 * yields nothing and pushes never happen. When the real pipeline is attached
 * the view is hydrated by pushing every row of the pipeline's initial fetch
 * through the normal `Output.push` path, so the view sees the data exactly as
 * it would see rows arriving incrementally.
 *
 * This lets `materialize()` stay synchronous and return a view immediately
 * while the expensive pipeline construction and hydration are postponed, for
 * example until the client's replica has been loaded into the IVM sources.
 */
export class DeferredInput implements Input {
  #output: Output | undefined;
  #input: Input | undefined;
  #destroyed = false;

  get attached(): boolean {
    return this.#input !== undefined;
  }

  get destroyed(): boolean {
    return this.#destroyed;
  }

  setOutput(output: Output): void {
    this.#output = output;
    this.#input?.setOutput(output);
  }

  getSchema(): SourceSchema {
    assert(
      this.#input,
      'DeferredInput: schema is not available before the pipeline is attached',
    );
    return this.#input.getSchema();
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    if (this.#input) {
      yield* this.#input.fetch(req);
    }
  }

  destroy(): void {
    this.#destroyed = true;
    this.#input?.destroy();
  }

  /**
   * Attach the real pipeline and hydrate the output with its current contents.
   *
   * Must be called inside the delegate's `batchViewUpdates`; the delegate is
   * responsible for notifying commit listeners afterwards so views flush.
   */
  attach(input: Input): void {
    assert(!this.#input, 'DeferredInput: pipeline already attached');
    assert(!this.#destroyed, 'DeferredInput: cannot attach after destroy');
    this.#input = input;
    const output = this.#output;
    assert(output, 'DeferredInput: attach called before setOutput');
    input.setOutput(output);
    for (const node of input.fetch({})) {
      if (node === 'yield') {
        continue;
      }
      consume(output.push(makeAddChange(node), input));
    }
  }
}
