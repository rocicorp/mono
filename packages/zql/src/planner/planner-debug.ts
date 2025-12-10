import type * as v from '../../../shared/src/valita.ts';
import type {
  Condition,
  Ordering,
  ValuePosition,
} from '../../../zero-protocol/src/ast.ts';
import type {
  attemptStartEventJSONSchema,
  bestPlanSelectedEventJSONSchema,
  connectionSelectedEventJSONSchema,
  nodeConstraintEventJSONSchema,
  PlanDebugEventJSON,
  planFailedEventJSONSchema,
} from '../../../zero-protocol/src/analyze-query-result.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import type {CostEstimate, JoinType} from './planner-node.ts';
import type {PlanState} from './planner-graph.ts';

/**
 * Structured debug events emitted during query planning.
 * These events can be accumulated, printed, or analyzed to understand
 * the planner's decision-making process.
 */

/**
 * Starting a new planning attempt with a different root connection.
 */
export type AttemptStartEvent = v.Infer<typeof attemptStartEventJSONSchema>;

/**
 * Snapshot of connection costs before selecting the next connection.
 */
export type ConnectionCostsEvent = {
  type: 'connection-costs';
  attemptNumber: number;
  costs: Array<{
    connection: string;
    cost: number;
    costEstimate: Omit<CostEstimate, 'fanout'>;
    pinned: boolean;
    constraints: Record<string, PlannerConstraint | undefined>;
    constraintCosts: Record<string, Omit<CostEstimate, 'fanout'>>;
  }>;
};

/**
 * A connection was chosen and pinned.
 */
export type ConnectionSelectedEvent = v.Infer<
  typeof connectionSelectedEventJSONSchema
>;

/**
 * Constraints have been propagated through the graph.
 */
export type ConstraintsPropagatedEvent = {
  type: 'constraints-propagated';
  attemptNumber: number;
  connectionConstraints: Array<{
    connection: string;
    constraints: Record<string, PlannerConstraint | undefined>;
    constraintCosts: Record<string, Omit<CostEstimate, 'fanout'>>;
  }>;
};

/**
 * A complete plan was found for this attempt.
 */
export type PlanCompleteEvent = {
  type: 'plan-complete';
  attemptNumber: number;
  totalCost: number;
  flipPattern: number; // Bitmask indicating which joins are flipped
  joinStates: Array<{
    join: string;
    type: JoinType;
  }>;
  // Planning snapshot that can be restored and applied to AST
  planSnapshot: PlanState;
};

/**
 * Planning attempt failed (e.g., unflippable join).
 */
export type PlanFailedEvent = v.Infer<typeof planFailedEventJSONSchema>;

/**
 * The best plan across all attempts was selected.
 */
export type BestPlanSelectedEvent = v.Infer<
  typeof bestPlanSelectedEventJSONSchema
>;

/**
 * A node computed its cost estimate during planning.
 * Emitted by nodes during estimateCost() traversal.
 * attemptNumber is added by the debugger.
 */
export type NodeCostEvent = {
  type: 'node-cost';
  attemptNumber?: number;
  nodeType: 'connection' | 'join' | 'fan-out' | 'fan-in' | 'terminus';
  node: string;
  branchPattern: number[];
  downstreamChildSelectivity: number;
  costEstimate: Omit<CostEstimate, 'fanout'>;
  filters?: Condition | undefined; // Only for connections
  ordering?: Ordering | undefined; // Only for connections
  joinType?: JoinType | undefined; // Only for joins
};

/**
 * A node received constraints during constraint propagation.
 * Emitted by nodes during propagateConstraints() traversal.
 * attemptNumber is added by the debugger.
 */
export type NodeConstraintEvent = v.Infer<typeof nodeConstraintEventJSONSchema>;

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
  | BestPlanSelectedEvent
  | NodeCostEvent
  | NodeConstraintEvent;

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
  private currentAttempt = 0;

  log(event: PlanDebugEvent): void {
    // Track current attempt number
    if (event.type === 'attempt-start') {
      this.currentAttempt = event.attemptNumber;
    }

    // Add attempt number to node events
    if (event.type === 'node-cost' || event.type === 'node-constraint') {
      (event as NodeCostEvent | NodeConstraintEvent).attemptNumber =
        this.currentAttempt;
    }

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
    return formatPlannerEvents(this.events);
  }
}

/**
 * Format a constraint object as a human-readable string.
 */
function formatConstraint(
  constraint: PlannerConstraint | Record<string, unknown> | null | undefined,
): string {
  if (!constraint) return '{}';
  const keys = Object.keys(constraint);
  if (keys.length === 0) return '{}';
  return '{' + keys.join(', ') + '}';
}

/**
 * Format a ValuePosition (column, literal, or static parameter) as a human-readable string.
 */
function formatValuePosition(value: ValuePosition): string {
  switch (value.type) {
    case 'column':
      return value.name;
    case 'literal':
      // Format literal values with SQL-style quoting for strings
      if (typeof value.value === 'string') {
        return `'${value.value}'`;
      }
      return JSON.stringify(value.value);
    case 'static':
      return `@${value.anchor}.${Array.isArray(value.field) ? value.field.join('.') : value.field}`;
  }
}

/**
 * Format a Condition (filter) as a human-readable string.
 */
function formatFilter(filter: Condition | undefined): string {
  if (!filter) return 'none';

  switch (filter.type) {
    case 'simple':
      return `${formatValuePosition(filter.left)} ${filter.op} ${formatValuePosition(filter.right)}`;
    case 'and':
      return `(${filter.conditions.map(formatFilter).join(' AND ')})`;
    case 'or':
      return `(${filter.conditions.map(formatFilter).join(' OR ')})`;
    case 'correlatedSubquery':
      return `EXISTS(${filter.related.subquery.table})`;
    default:
      return JSON.stringify(filter);
  }
}

/**
 * Format an Ordering as a human-readable string.
 */
function formatOrdering(ordering: Ordering | undefined): string {
  if (!ordering || ordering.length === 0) return 'none';
  return ordering
    .map(([field, direction]) => `${field} ${direction}`)
    .join(', ');
}

/**
 * Format a compact summary for a single planning attempt.
 */
function formatAttemptSummary(
  attemptNum: number,
  events: (PlanDebugEvent | PlanDebugEventJSON)[],
): string[] {
  const lines: string[] = [];

  // Find the attempt-start event to get total attempts
  const startEvent = events.find(e => e.type === 'attempt-start') as
    | AttemptStartEvent
    | undefined;
  const totalAttempts = startEvent?.totalAttempts ?? '?';

  // Calculate number of bits needed for pattern
  const numBits =
    typeof totalAttempts === 'number'
      ? Math.ceil(Math.log2(totalAttempts)) || 1
      : 1;
  const bitPattern = attemptNum.toString(2).padStart(numBits, '0');

  lines.push(
    `[Attempt ${attemptNum + 1}/${totalAttempts}] Pattern ${attemptNum} (${bitPattern})`,
  );

  // Collect connection costs (use array to preserve all connections, including duplicates)
  const connectionCostEvents: (
    | NodeCostEvent
    | Extract<PlanDebugEventJSON, {type: 'node-cost'}>
  )[] = [];
  const connectionConstraintEvents: (
    | NodeConstraintEvent
    | Extract<PlanDebugEventJSON, {type: 'node-constraint'}>
  )[] = [];

  for (const event of events) {
    if (event.type === 'node-cost' && event.nodeType === 'connection') {
      connectionCostEvents.push(event);
    }
    if (event.type === 'node-constraint' && event.nodeType === 'connection') {
      connectionConstraintEvents.push(event);
    }
  }

  // Show connection summary
  if (connectionCostEvents.length > 0) {
    lines.push('  Connections:');
    for (const cost of connectionCostEvents) {
      // Find matching constraint event (same node name and branch pattern)
      const constraint = connectionConstraintEvents.find(
        c =>
          c.node === cost.node &&
          c.branchPattern.join(',') === cost.branchPattern.join(','),
      )?.constraint;

      const constraintStr = formatConstraint(constraint);
      const filterStr = formatFilter(cost.filters);
      const orderingStr = formatOrdering(cost.ordering);
      const limitStr =
        cost.costEstimate.limit !== undefined
          ? cost.costEstimate.limit.toString()
          : 'none';

      lines.push(`    ${cost.node}:`);
      lines.push(
        `      cost=${cost.costEstimate.cost.toFixed(2)}, startup=${cost.costEstimate.startupCost.toFixed(2)}, scan=${cost.costEstimate.scanEst.toFixed(2)}`,
      );
      lines.push(
        `      rows=${cost.costEstimate.returnedRows.toFixed(2)}, selectivity=${cost.costEstimate.selectivity.toFixed(8)}, limit=${limitStr}`,
      );
      lines.push(
        `      downstreamChildSelectivity=${cost.downstreamChildSelectivity.toFixed(8)}`,
      );
      lines.push(`      constraints=${constraintStr}`);
      lines.push(`      filters=${filterStr}`);
      lines.push(`      ordering=${orderingStr}`);
    }
  }

  // Collect join costs from node-cost events
  const joinCosts: (
    | NodeCostEvent
    | Extract<PlanDebugEventJSON, {type: 'node-cost'}>
  )[] = [];
  for (const event of events) {
    if (event.type === 'node-cost' && event.nodeType === 'join') {
      joinCosts.push(event);
    }
  }

  if (joinCosts.length > 0) {
    lines.push('  Joins:');
    for (const cost of joinCosts) {
      const typeStr = cost.joinType ? ` (${cost.joinType})` : '';
      const limitStr =
        cost.costEstimate.limit !== undefined
          ? cost.costEstimate.limit.toString()
          : 'none';

      lines.push(`    ${cost.node}${typeStr}:`);
      lines.push(
        `      cost=${cost.costEstimate.cost.toFixed(2)}, startup=${cost.costEstimate.startupCost.toFixed(2)}, scan=${cost.costEstimate.scanEst.toFixed(2)}`,
      );
      lines.push(
        `      rows=${cost.costEstimate.returnedRows.toFixed(2)}, selectivity=${cost.costEstimate.selectivity.toFixed(8)}, limit=${limitStr}`,
      );
      lines.push(
        `      downstreamChildSelectivity=${cost.downstreamChildSelectivity.toFixed(8)}`,
      );
    }
  }

  // Find completion/failure events
  const completeEvent = events.find(e => e.type === 'plan-complete') as
    | PlanCompleteEvent
    | undefined;
  const failedEvent = events.find(e => e.type === 'plan-failed') as
    | PlanFailedEvent
    | undefined;

  // Show final status

  if (completeEvent) {
    lines.push(
      `  ✓ Plan complete: total cost = ${completeEvent.totalCost.toFixed(2)}`,
    );
  } else if (failedEvent) {
    lines.push(`  ✗ Plan failed: ${failedEvent.reason}`);
  }

  return lines;
}

/**
 * Convert undefined values to null in a constraint object for JSON serialization.
 * PlannerConstraint uses Record<string, undefined> which loses keys during JSON.stringify.
 */
function convertConstraintUndefinedToNull(
  constraint: PlannerConstraint | Record<string, unknown> | undefined | null,
): Record<string, unknown> | undefined | null {
  if (constraint === undefined) {
    return undefined;
  }
  if (constraint === null) {
    return null;
  }
  const result: Record<string, unknown> = {};
  for (const [key, val] of Object.entries(constraint)) {
    result[key] = val === undefined ? null : val;
  }
  return result;
}

/**
 * Serialize a single debug event to JSON-compatible format.
 * The fanout function is already omitted when events are created.
 * The planSnapshot is excluded as it's internal state not needed for debugging.
 * Undefined values in constraints are converted to null for JSON serialization.
 */
function serializeEvent(event: PlanDebugEvent): PlanDebugEventJSON {
  // Remove planSnapshot from plan-complete events
  if (event.type === 'plan-complete') {
    const {planSnapshot: _, ...rest} = event;
    return rest as PlanDebugEventJSON;
  }

  // Convert constraint undefined values to null for specific event types
  if (event.type === 'node-constraint') {
    return {
      ...event,
      constraint: convertConstraintUndefinedToNull(event.constraint),
    } as PlanDebugEventJSON;
  }

  if (event.type === 'connection-costs') {
    return {
      ...event,
      costs: event.costs.map(cost => ({
        ...cost,
        constraints: Object.fromEntries(
          Object.entries(cost.constraints).map(([key, val]) => [
            key,
            convertConstraintUndefinedToNull(val),
          ]),
        ),
      })),
    } as PlanDebugEventJSON;
  }

  if (event.type === 'constraints-propagated') {
    return {
      ...event,
      connectionConstraints: event.connectionConstraints.map(cc => ({
        ...cc,
        constraints: Object.fromEntries(
          Object.entries(cc.constraints).map(([key, val]) => [
            key,
            convertConstraintUndefinedToNull(val),
          ]),
        ),
      })),
    } as PlanDebugEventJSON;
  }

  return event as PlanDebugEventJSON;
}

/**
 * Serialize an array of debug events to JSON-compatible format.
 * The fanout function is already omitted when events are created.
 * The planSnapshot is excluded as it's internal state not needed for debugging.
 */
export function serializePlanDebugEvents(
  events: PlanDebugEvent[],
): PlanDebugEventJSON[] {
  return events.map(serializeEvent);
}

/**
 * Format planner debug events as a human-readable string.
 * Works with JSON-serialized events (from inspector API) or native events (from AccumulatorDebugger).
 *
 * @param events - Array of planner debug events (either JSON or native format)
 * @returns Formatted string showing planning attempts, costs, and final plan selection
 *
 * @example
 * ```typescript
 * const result = await inspector.analyzeQuery(query, { joinPlans: true });
 * if (result.joinPlans) {
 *   console.log(formatPlannerEvents(result.joinPlans));
 * }
 * ```
 */
export function formatPlannerEvents(
  events: PlanDebugEventJSON[] | PlanDebugEvent[],
): string {
  const lines: string[] = [];

  // Group events by attempt
  const eventsByAttempt = new Map<
    number,
    (PlanDebugEventJSON | PlanDebugEvent)[]
  >();
  let bestPlanEvent:
    | {
        type: 'best-plan-selected';
        bestAttemptNumber: number;
        totalCost: number;
        flipPattern: number;
        joinStates: Array<{join: string; type: string}>;
      }
    | undefined;

  for (const event of events) {
    if ('attemptNumber' in event) {
      const attempt = event.attemptNumber;
      if (attempt !== undefined) {
        let attemptEvents = eventsByAttempt.get(attempt);
        if (!attemptEvents) {
          attemptEvents = [];
          eventsByAttempt.set(attempt, attemptEvents);
        }
        attemptEvents.push(event);
      }
    } else if (event.type === 'best-plan-selected') {
      // Save for displaying at the end
      bestPlanEvent = event;
    }
  }

  // Format each attempt as a compact summary
  for (const [attemptNum, events] of eventsByAttempt.entries()) {
    lines.push(...formatAttemptSummary(attemptNum, events));
    lines.push(''); // Blank line between attempts
  }

  // Show the final plan selection
  if (bestPlanEvent) {
    lines.push('─'.repeat(60));
    lines.push(
      `✓ Best plan: Attempt ${bestPlanEvent.bestAttemptNumber + 1} (cost=${bestPlanEvent.totalCost.toFixed(2)})`,
    );
    if (bestPlanEvent.joinStates.length > 0) {
      lines.push('  Join types:');
      for (const j of bestPlanEvent.joinStates) {
        lines.push(`    ${j.join}: ${j.type}`);
      }
    }
    lines.push('─'.repeat(60));
  }

  return lines.join('\n');
}
