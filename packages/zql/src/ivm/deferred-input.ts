import {assert} from '../../../shared/src/asserts.ts';
import {makeAddChange} from './change.ts';
import type {Node} from './data.ts';
import type {FetchRequest, Input, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {consume, type Stream} from './stream.ts';

/**
 * A placeholder `Input` for a view whose pipeline has not been built yet.
 *
 * Until {@link attach} is called it behaves like an empty pipeline: `fetch`
 * yields nothing and pushes never happen. When the real pipeline is attached
 * the view is hydrated with a single `fetch`, whose rows are delivered to the
 * view as add changes, so the view sees the data exactly as it would see rows
 * arriving incrementally.
 *
 * This lets `materialize()` stay synchronous and return a view immediately
 * while the expensive pipeline construction and hydration are postponed, for
 * example until the client's replica has been loaded into the IVM sources.
 */
export class DeferredInput implements Input {
  readonly #schema: SourceSchema;
  readonly #build: () => Input;
  #output: Output | undefined;
  #input: Input | undefined;
  #destroyed = false;

  /**
   * @param schema The schema the built pipeline reports. Passed in so this
   *   input can answer `getSchema()` unconditionally, as the `Input` contract
   *   requires, before its pipeline exists.
   * @param build Builds the real pipeline. Called once, from {@link attach}.
   */
  constructor(schema: SourceSchema, build: () => Input) {
    this.#schema = schema;
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
    return this.#schema;
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
   *
   * Must be called inside the delegate's `batchViewUpdates`; the delegate is
   * responsible for notifying commit listeners afterwards so views flush.
   *
   * If building or hydrating throws, the pipeline is torn down again and this
   * input stays unattached so the caller can report the failure.
   */
  attach(): void {
    assert(!this.#input, 'DeferredInput: pipeline already attached');
    assert(!this.#destroyed, 'DeferredInput: cannot attach after destroy');
    const input = this.#build();
    const output = this.#output;
    if (!output) {
      // The view never asked for output; there is nothing to hydrate.
      this.#input = input;
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
      input.destroy();
      throw e;
    }
    this.#input = input;
  }
}
