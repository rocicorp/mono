import type {PlannerConstraint} from './planner-constraint.ts';
import type {PlannerNode} from './planner-node.ts';

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
  ): void {
    if (this.#type === 'FI') {
      const updatedPattern = [0, ...branchPattern];
      /**
       * All inputs get the same branch pattern.
       * 1. They cannot contribute differing constraints to their parent inputs because they are not flipped.
       *    If they were flipped this would be of type UFI.
       * 2. All inputs need to be called because they could be pinned. If they are pinned they have constraints
       *    to send to their children.
       */
      for (const input of this.#inputs) {
        input.propagateConstraints(updatedPattern, constraint);
      }
      return;
    }

    let i = 0;
    for (const input of this.#inputs) {
      input.propagateConstraints([i, ...branchPattern], constraint);
      i++;
    }
  }
}
