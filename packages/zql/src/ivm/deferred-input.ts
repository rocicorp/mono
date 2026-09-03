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
  readonly #build: () => Input;
  #output: Output | undefined;
  #input: Input | undefined;
  #destroyed = false;

  /**
   * @param build Builds the real pipeline. Called at most once, either from
   *   {@link attach} or, if the view needs the schema before the delegate is
   *   ready, from {@link getSchema}.
   */
  constructor(build: () => Input) {
    this.#build = build;
  }

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
    if (!this.#input) {
      // Honor the Input contract for views that read the schema before the
      // delegate is ready: build the pipeline now. The view has not fetched
      // yet, so it hydrates through the normal fetch path and receives later
      // pushes as usual. Only the cold-boot optimization is lost for this view.
      assert(!this.#destroyed, 'DeferredInput: destroyed');
      const input = this.#build();
      this.#input = input;
      if (this.#output) {
        input.setOutput(this.#output);
      }
    }
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
   * Build the real pipeline and hydrate the output with its current contents.
   * A no-op if the pipeline was already built by {@link getSchema}.
   *
   * Must be called inside the delegate's `batchViewUpdates`; the delegate is
   * responsible for notifying commit listeners afterwards so views flush.
   *
   * If building or hydrating throws, the pipeline is torn down again and the
   * input stays unattached so the caller can report the failure.
   */
  attach(): void {
    assert(!this.#destroyed, 'DeferredInput: cannot attach after destroy');
    if (this.#input) {
      return;
    }
    const input = this.#build();
    // Assign before hydrating: the view may call getSchema() while applying
    // the pushed rows and must see this pipeline, not build another.
    this.#input = input;
    const output = this.#output;
    if (!output) {
      // The view never asked for output; there is nothing to hydrate.
      return;
    }
    try {
      input.setOutput(output);
      for (const node of input.fetch({})) {
        if (node === 'yield') {
          continue;
        }
        consume(output.push(makeAddChange(node), input));
      }
    } catch (e) {
      this.#input = undefined;
      input.destroy();
      throw e;
    }
  }
}
