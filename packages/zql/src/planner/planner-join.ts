import {assert} from '../../../shared/src/asserts.ts';
import {planIdSymbol} from '../../../zero-protocol/src/ast.ts';
import {
  mergeConstraints,
  type PlannerConstraint,
} from './planner-constraint.ts';
import {UnflippableJoinError} from './planner-graph.ts';
import type {FromType, PlannerNode} from './planner-node.ts';

export class PlannerJoin {
  readonly kind = 'join' as const;
  #type: 'left' | 'flipped';
  #pinned: boolean;
  #output?: PlannerNode | undefined;
  readonly #parent: PlannerNode;
  readonly #child: PlannerNode;
  readonly #parentConstraint: PlannerConstraint;
  readonly #childConstraint: PlannerConstraint;
  readonly #flippable: boolean;
  readonly [planIdSymbol]?: number | undefined;

  constructor(
    parent: PlannerNode,
    child: PlannerNode,
    parentConstraint: PlannerConstraint,
    childConstraint: PlannerConstraint,
    flippable: boolean,
    planId?: number | undefined,
  ) {
    this.#type = 'left';
    this.#pinned = false;
    this.#parent = parent;
    this.#child = child;
    this.#childConstraint = childConstraint;
    this.#parentConstraint = parentConstraint;
    this.#flippable = flippable;
    this[planIdSymbol] = planId;
  }

  setOutput(node: PlannerNode): void {
    this.#output = node;
  }

  get output(): PlannerNode {
    assert(this.#output !== undefined, 'Output not set');
    return this.#output;
  }

  maybeFlip(input: PlannerNode): void {
    assert(this.#pinned === false, 'Cannot flip a pinned join');
    if (input === this.#child) {
      this.flip();
    } else {
      assert(
        input === this.#parent,
        'Can only flip a join from one of its inputs',
      );
    }
  }

  flip(): void {
    assert(this.#type === 'left', 'Can only flip a left join');
    assert(this.#pinned === false, 'Cannot flip a pinned join');
    if (!this.#flippable) {
      throw new UnflippableJoinError(
        'Cannot flip a non-flippable join (e.g., NOT EXISTS)',
      );
    }
    this.#type = 'flipped';
  }

  get type(): 'left' | 'flipped' {
    return this.#type;
  }

  pin(): void {
    assert(this.#pinned === false, 'Cannot pin a pinned join');
    this.#pinned = true;
  }

  get pinned(): boolean {
    return this.#pinned;
  }

  get planId(): number | undefined {
    return this[planIdSymbol];
  }

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    from: FromType,
  ): void {
    if (this.#pinned) {
      assert(
        from === 'pinned',
        'It should be impossible for a pinned join to receive constraints from a non-pinned node',
      );
    }

    if (this.#pinned && this.#type === 'left') {
      // A left join always has constraints for its child.
      // They are defined by the correlated between parent and child.
      this.#child.propagateConstraints(
        branchPattern,
        this.#childConstraint,
        'pinned',
      );
      // A left join forwards constraints to its parent.
      this.#parent.propagateConstraints(branchPattern, constraint, 'pinned');
    }
    if (this.#pinned && this.#type === 'flipped') {
      // A flipped join has no constraints to pass to its child.
      // It is a standalone fetch that is relying on the filters of the child
      // connection to do the heavy work.
      this.#child.propagateConstraints(branchPattern, undefined, 'pinned');
      // A flipped join will have constraints to send to its parent.
      // - The constraints its output sent
      // - The constraints its child creates
      this.#parent.propagateConstraints(
        branchPattern,
        mergeConstraints(constraint, this.#parentConstraint),
        'pinned',
      );
    }
    if (!this.#pinned && this.#type === 'left') {
      // If a join is not pinned, it cannot contribute constraints to its child.
      // Contributing constraints to its child would reduce the child's cost too early
      // causing the child to be picked by the planning algorithm before the parent
      // that is contributing the constraints has been picked.
      this.#parent.propagateConstraints(branchPattern, constraint, 'unpinned');
    }
    if (!this.#pinned && this.#type === 'flipped') {
      // If a join has been flipped that means it has been picked by the planning algorithm.
      // If it has been picked, it must be pinned.
      throw new Error('Impossible to be flipped and not pinned');
    }
  }

  reset(): void {
    this.#type = 'left';
    this.#pinned = false;
  }
}
