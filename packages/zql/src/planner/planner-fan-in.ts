import type {PlannerConstraint} from './planner-constraint.ts';
import type {FromType, PlannerNode} from './planner-node.ts';

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
export class PlannerFanIn implements PlannerNode {
  #type: 'FI' | 'UFI';
  readonly #inputs: PlannerNode[];

  constructor(inputs: PlannerNode[]) {
    this.#type = 'FI';
    this.#inputs = inputs;
  }

  get type() {
    return this.#type;
  }

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    from: FromType,
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
        input.propagateConstraints(updatedPattern, constraint, from);
      }
      return;
    }

    let i = 0;
    for (const input of this.#inputs) {
      input.propagateConstraints([i, ...branchPattern], constraint, from);
      i++;
    }
  }
}
