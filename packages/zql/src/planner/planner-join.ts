import {assert} from '../../../shared/src/asserts.ts';
import {
  mergeConstraints,
  type PlannerConstraint,
} from './planner-constraint.ts';
import type {ConstraintPropagationType, PlannerNode} from './planner-node.ts';

/**
 * Represents a join between two data streams (parent and child).
 *
 * # Dual-State Pattern
 * Like all planner nodes, PlannerJoin separates:
 * 1. IMMUTABLE STRUCTURE: Parent/child nodes, constraints, flippability
 * 2. MUTABLE STATE: Join type (left/flipped), pinned status
 *
 * # Join Flipping
 * A join can be in two states:
 * - 'left': Parent is outer loop, child is inner
 * - 'flipped': Child is outer loop, parent is inner
 *
 * Flipping is the key optimization: choosing which table scans first.
 * NOT EXISTS joins cannot be flipped (#flippable = false).
 *
 * # Constraint Propagation
 * - Left join: Sends childConstraint to child, forwards received constraints to parent
 * - Flipped join: Sends undefined to child, merges parentConstraint with received to parent
 * - Unpinned join: Only forwards constraints to parent (doesn't constrain child yet)
 *
 * # Lifecycle
 * 1. Construct with immutable structure (parent, child, constraints, flippability)
 * 2. Wire to output node during graph construction
 * 3. Planning calls flipIfNeeded() based on connection selection order
 * 4. pin() locks the join type once chosen
 * 5. reset() clears mutable state (type → 'left', pinned → false)
 */
export class PlannerJoin {
  readonly kind = 'join' as const;

  // ========================================================================
  // IMMUTABLE STRUCTURE (set during construction, never changes)
  // ========================================================================
  readonly #parent: PlannerNode;
  readonly #child: PlannerNode;
  readonly #parentConstraint: PlannerConstraint;
  readonly #childConstraint: PlannerConstraint;
  readonly #flippable: boolean;
  readonly planId: number;
  #output?: PlannerNode | undefined; // Set once during graph construction

  // ========================================================================
  // MUTABLE PLANNING STATE (changes during plan search)
  // ========================================================================
  #type: 'left' | 'flipped';
  #pinned: boolean;

  constructor(
    parent: PlannerNode,
    child: PlannerNode,
    parentConstraint: PlannerConstraint,
    childConstraint: PlannerConstraint,
    flippable: boolean,
    planId: number,
  ) {
    this.#type = 'left';
    this.#pinned = false;
    this.#parent = parent;
    this.#child = child;
    this.#childConstraint = childConstraint;
    this.#parentConstraint = parentConstraint;
    this.#flippable = flippable;
    this.planId = planId;
  }

  setOutput(node: PlannerNode): void {
    this.#output = node;
  }

  get output(): PlannerNode {
    assert(this.#output !== undefined, 'Output not set');
    return this.#output;
  }

  flipIfNeeded(input: PlannerNode): void {
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

  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    from: ConstraintPropagationType,
  ): void {
    if (this.#pinned) {
      assert(
        from === 'pinned' || from === 'terminus',
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

export class UnflippableJoinError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'UnflippableJoinError';
  }
}
