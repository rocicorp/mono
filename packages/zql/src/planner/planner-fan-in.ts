import {assert} from '../../../shared/src/asserts.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import type {ConstraintPropagationType, PlannerNode} from './planner-node.ts';

/**
 * A PlannerFanIn node can either be a normal FanIn or UnionFanIn.
 *
 * These have different performance characteristics so we need to distinguish them.
 *
 * A normal FanIn only does a single fetch to FanOut, regardless of how many internal
 * branches / inputs it has.
 *
 * A UnionFanIn does a fetch per internal branch / input. This causes an exponential
 * increase in cost if many UnionFanIns are chained after on another. E.g., `(A or B) AND (C or D)`.
 *
 * To capture this cost blow-up, union fan in assigns different branch patterns to their inputs.
 *
 * Since UFI will generate a unique branch pattern per input, planner-connection will yield a higher cost
 * each time a UFI is present. planner-connection will return the sum of the costs of each unique branch pattern.
 */
export class PlannerFanIn {
  readonly kind = 'fan-in' as const;
  #type: 'FI' | 'UFI';
  #output?: PlannerNode | undefined;
  readonly #inputs: PlannerNode[];

  constructor(inputs: PlannerNode[]) {
    this.#type = 'FI';
    this.#inputs = inputs;
  }

  get type() {
    return this.#type;
  }

  setOutput(node: PlannerNode): void {
    this.#output = node;
  }

  get output(): PlannerNode {
    assert(this.#output !== undefined, 'Output not set');
    return this.#output;
  }

  reset() {
    this.#type = 'FI';
  }

  convertToUFI(): void {
    this.#type = 'UFI';
  }

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    from: ConstraintPropagationType,
  ): void {
    if (this.#type === 'FI') {
      const updatedPattern = [0, ...branchPattern];
      /**
       * All inputs get the same branch pattern.
       * 1. They cannot contribute differing constraints to their parent inputs because they are not flipped.
       *    If they were flipped this would be of type UFI.
       * 2. All inputs need to be called because they could be pinned. If they are pinned they could have constraints
       *    to send to their children.
       */
      for (const input of this.#inputs) {
        // Check if this input is pinned and adjust the 'from' value accordingly
        const inputFrom =
          input.kind === 'join' && input.pinned ? 'pinned' : from;
        input.propagateConstraints(updatedPattern, constraint, inputFrom);
      }
      return;
    }

    let i = 0;
    for (const input of this.#inputs) {
      // Check if this input is pinned and adjust the 'from' value accordingly
      const inputFrom = input.kind === 'join' && input.pinned ? 'pinned' : from;
      input.propagateConstraints([i, ...branchPattern], constraint, inputFrom);
      i++;
    }
  }
}
