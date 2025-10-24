import type {PlannerConstraint} from './planner-constraint.ts';
import type {CostEstimate, JoinType, NodeType} from './planner-node.ts';

/**
 * Structured debug events emitted during query planning.
 * These events can be accumulated, printed, or analyzed to understand
 * the planner's decision-making process.
 */

/**
 * Starting a new planning attempt with a different root connection.
 */
export type AttemptStartEvent = {
  type: 'attempt-start';
  attemptNumber: number;
  totalAttempts: number;
};

/**
 * Snapshot of connection costs before selecting the next connection.
 */
export type ConnectionCostsEvent = {
  type: 'connection-costs';
  attemptNumber: number;
  costs: Array<{
    connection: string;
    cost: number;
    costEstimate: CostEstimate;
    pinned: boolean;
    constraints: Map<string, PlannerConstraint | undefined>;
  }>;
};

/**
 * A connection was chosen and pinned.
 */
export type ConnectionSelectedEvent = {
  type: 'connection-selected';
  attemptNumber: number;
  connection: string;
  cost: number;
  isRoot: boolean; // First connection in this attempt
};

/**
 * Constraints have been propagated through the graph.
 */
export type ConstraintsPropagatedEvent = {
  type: 'constraints-propagated';
  attemptNumber: number;
  connectionConstraints: Array<{
    connection: string;
    constraints: Map<string, PlannerConstraint | undefined>;
  }>;
};

/**
 * A complete plan was found for this attempt.
 */
export type PlanCompleteEvent = {
  type: 'plan-complete';
  attemptNumber: number;
  totalCost: number;
  nodeCosts: Array<{
    node: string;
    nodeType: NodeType;
    costEstimate: CostEstimate;
  }>;
  joinStates: Array<{
    join: string;
    type: JoinType;
    pinned: boolean;
  }>;
};

/**
 * Planning attempt failed (e.g., unflippable join).
 */
export type PlanFailedEvent = {
  type: 'plan-failed';
  attemptNumber: number;
  reason: string;
};

/**
 * The best plan across all attempts was selected.
 */
export type BestPlanSelectedEvent = {
  type: 'best-plan-selected';
  bestAttemptNumber: number;
  totalCost: number;
  joinStates: Array<{
    join: string;
    type: JoinType;
  }>;
};

/**
 * Union of all debug event types.
 */
export type PlanDebugEvent =
  | AttemptStartEvent
  | ConnectionCostsEvent
  | ConnectionSelectedEvent
  | ConstraintsPropagatedEvent
  | PlanCompleteEvent
  | PlanFailedEvent
  | BestPlanSelectedEvent;

/**
 * Interface for objects that receive debug events during planning.
 */
export interface PlanDebugger {
  log(event: PlanDebugEvent): void;
}

/**
 * Simple accumulator debugger that stores all events.
 * Useful for tests and debugging.
 */
export class AccumulatorDebugger implements PlanDebugger {
  readonly events: PlanDebugEvent[] = [];

  log(event: PlanDebugEvent): void {
    this.events.push(event);
  }

  /**
   * Get all events of a specific type.
   */
  getEvents<T extends PlanDebugEvent['type']>(
    type: T,
  ): Extract<PlanDebugEvent, {type: T}>[] {
    return this.events.filter(e => e.type === type) as Extract<
      PlanDebugEvent,
      {type: T}
    >[];
  }

  /**
   * Format events as a human-readable string.
   */
  format(): string {
    const lines: string[] = [];
    for (const event of this.events) {
      lines.push(formatEvent(event));
    }
    return lines.join('\n');
  }
}

/**
 * Format a constraint object as a human-readable string.
 */
function formatConstraint(constraint: PlannerConstraint | undefined): string {
  if (!constraint) return '{}';
  const keys = Object.keys(constraint);
  if (keys.length === 0) return '{}';
  return '{' + keys.join(', ') + '}';
}

/**
 * Format a single debug event as a human-readable string.
 */
function formatEvent(event: PlanDebugEvent): string {
  switch (event.type) {
    case 'attempt-start':
      return `[Attempt ${event.attemptNumber + 1}/${event.totalAttempts}] Starting planning attempt`;

    case 'connection-costs': {
      const lines = [`[Attempt ${event.attemptNumber + 1}] Connection costs:`];
      for (const c of event.costs) {
        // Format the main connection info
        // const limitStr = c.costEstimate.limit !== undefined
        //   ? c.costEstimate.limit.toString()
        //   : 'none';
        lines.push(
          `  ${c.connection}: cost=${c.cost.toFixed(2)}, `,
          // `selectivity=${c.costEstimate.selectivity.toFixed(3)}, ` +
          // `limit=${limitStr}`,
        );

        // Format each branch's constraints
        if (c.constraints.size === 0) {
          lines.push(`    Branch [none]: {}`);
        } else {
          for (const [branchKey, constraint] of c.constraints) {
            const branchLabel = branchKey === '' ? 'none' : branchKey;
            lines.push(
              `    Branch [${branchLabel}]: ${formatConstraint(constraint)}`,
            );
          }
        }
      }
      return lines.join('\n');
    }

    case 'connection-selected':
      return (
        `[Attempt ${event.attemptNumber + 1}] Selected ${event.isRoot ? 'ROOT' : ''} connection: ` +
        `${event.connection} (cost=${event.cost.toFixed(2)})`
      );

    case 'constraints-propagated': {
      const lines = [
        `[Attempt ${event.attemptNumber + 1}] Constraints propagated:`,
      ];
      for (const c of event.connectionConstraints) {
        lines.push(`  ${c.connection}:`);

        // Format each branch's constraints
        if (c.constraints.size === 0) {
          lines.push(`    Branch [none]: {}`);
        } else {
          for (const [branchKey, constraint] of c.constraints) {
            const branchLabel = branchKey === '' ? 'none' : branchKey;
            lines.push(
              `    Branch [${branchLabel}]: ${formatConstraint(constraint)}`,
            );
          }
        }
      }
      return lines.join('\n');
    }

    case 'plan-complete': {
      const lines = [
        `[Attempt ${event.attemptNumber + 1}] Plan complete! Total cost: ${event.totalCost.toFixed(2)}`,
        `  Joins:`,
      ];
      for (const j of event.joinStates) {
        lines.push(`    ${j.join}: ${j.type}`);
      }
      lines.push(`  Node costs:`);
      for (const n of event.nodeCosts) {
        lines.push(
          `    ${n.node} (${n.nodeType}): ${n.costEstimate.runningCost.toFixed(2)}`,
        );
      }
      return lines.join('\n');
    }

    case 'plan-failed':
      return `[Attempt ${event.attemptNumber + 1}] Plan FAILED: ${event.reason}`;

    case 'best-plan-selected': {
      const lines = [
        `[FINAL] Best plan selected from attempt ${event.bestAttemptNumber + 1}`,
        `  Total cost: ${event.totalCost.toFixed(2)}`,
        `  Joins:`,
      ];
      for (const j of event.joinStates) {
        lines.push(`    ${j.join}: ${j.type}`);
      }
      return lines.join('\n');
    }
  }
}
