import * as v from '../../shared/src/valita.ts';

import {conditionSchema, orderingSchema} from './ast.ts';
import {rowSchema} from './data.ts';

export const rowCountsByQuerySchema = v.record(v.number());
export type RowCountsByQuery = v.Infer<typeof rowCountsByQuerySchema>;

export const rowCountsBySourceSchema = v.record(rowCountsByQuerySchema);
export type RowCountsBySource = v.Infer<typeof rowCountsBySourceSchema>;

export const rowsByQuerySchema = v.record(v.array(rowSchema));
export type RowsByQuery = v.Infer<typeof rowsByQuerySchema>;

export const rowsBySourceSchema = v.record(rowsByQuerySchema);
export type RowsBySource = v.Infer<typeof rowsBySourceSchema>;

// Planner debug event schemas

const costEstimateJSONSchema = v.object({
  startupCost: v.number(),
  scanEst: v.number(),
  cost: v.number(),
  returnedRows: v.number(),
  selectivity: v.number(),
  limit: v.number().optional(),
});

const plannerConstraintSchema = v.record(v.union(v.unknown(), v.null()));

const attemptStartEventJSONSchema = v.object({
  type: v.literal('attempt-start'),
  attemptNumber: v.number(),
  totalAttempts: v.number(),
});

const connectionCostsEventJSONSchema = v.object({
  type: v.literal('connection-costs'),
  attemptNumber: v.number(),
  costs: v.array(
    v.object({
      connection: v.string(),
      cost: v.number(),
      costEstimate: costEstimateJSONSchema,
      pinned: v.boolean(),
      constraints: v.record(v.union(plannerConstraintSchema, v.null())),
      constraintCosts: v.record(costEstimateJSONSchema),
    }),
  ),
});

const connectionSelectedEventJSONSchema = v.object({
  type: v.literal('connection-selected'),
  attemptNumber: v.number(),
  connection: v.string(),
  cost: v.number(),
  isRoot: v.boolean(),
});

const constraintsPropagatedEventJSONSchema = v.object({
  type: v.literal('constraints-propagated'),
  attemptNumber: v.number(),
  connectionConstraints: v.array(
    v.object({
      connection: v.string(),
      constraints: v.record(v.union(plannerConstraintSchema, v.null())),
      constraintCosts: v.record(costEstimateJSONSchema),
    }),
  ),
});

const joinTypeSchema = v.union(
  v.literal('semi'),
  v.literal('flipped'),
  v.literal('unflippable'),
);

const planCompleteEventJSONSchema = v.object({
  type: v.literal('plan-complete'),
  attemptNumber: v.number(),
  totalCost: v.number(),
  flipPattern: v.number(),
  joinStates: v.array(
    v.object({
      join: v.string(),
      type: joinTypeSchema,
    }),
  ),
});

const planFailedEventJSONSchema = v.object({
  type: v.literal('plan-failed'),
  attemptNumber: v.number(),
  reason: v.string(),
});

const bestPlanSelectedEventJSONSchema = v.object({
  type: v.literal('best-plan-selected'),
  bestAttemptNumber: v.number(),
  totalCost: v.number(),
  flipPattern: v.number(),
  joinStates: v.array(
    v.object({
      join: v.string(),
      type: joinTypeSchema,
    }),
  ),
});

const nodeTypeSchema = v.union(
  v.literal('connection'),
  v.literal('join'),
  v.literal('fan-out'),
  v.literal('fan-in'),
  v.literal('terminus'),
);

const nodeCostEventJSONSchema = v.object({
  type: v.literal('node-cost'),
  attemptNumber: v.number().optional(),
  nodeType: nodeTypeSchema,
  node: v.string(),
  branchPattern: v.array(v.number()),
  downstreamChildSelectivity: v.number(),
  costEstimate: costEstimateJSONSchema,
  filters: conditionSchema.optional(),
  ordering: orderingSchema.optional(),
  joinType: joinTypeSchema.optional(),
});

const nodeConstraintEventJSONSchema = v.object({
  type: v.literal('node-constraint'),
  attemptNumber: v.number().optional(),
  nodeType: nodeTypeSchema,
  node: v.string(),
  branchPattern: v.array(v.number()),
  constraint: v.union(plannerConstraintSchema, v.null()).optional(),
  from: v.string(),
});

const planDebugEventJSONSchema = v.union(
  attemptStartEventJSONSchema,
  connectionCostsEventJSONSchema,
  connectionSelectedEventJSONSchema,
  constraintsPropagatedEventJSONSchema,
  planCompleteEventJSONSchema,
  planFailedEventJSONSchema,
  bestPlanSelectedEventJSONSchema,
  nodeCostEventJSONSchema,
  nodeConstraintEventJSONSchema,
);

export type PlanDebugEventJSON = v.Infer<typeof planDebugEventJSONSchema>;

export {
  attemptStartEventJSONSchema,
  connectionSelectedEventJSONSchema,
  planFailedEventJSONSchema,
  bestPlanSelectedEventJSONSchema,
  nodeConstraintEventJSONSchema,
};

export const analyzeQueryResultSchema = v.object({
  warnings: v.array(v.string()),
  syncedRows: v.record(v.array(rowSchema)).optional(),
  syncedRowCount: v.number(),
  start: v.number(),
  /** @deprecated Use start + elapsed instead */
  end: v.number(),
  elapsed: v.number().optional(),
  afterPermissions: v.string().optional(),
  /** @deprecated Use readRowCountsByQuery */
  vendedRowCounts: rowCountsBySourceSchema.optional(),
  /** @deprecated Use readRows */
  vendedRows: rowsBySourceSchema.optional(),
  sqlitePlans: v.record(v.array(v.string())).optional(),
  readRows: rowsBySourceSchema.optional(),
  readRowCountsByQuery: rowCountsBySourceSchema.optional(),
  readRowCount: v.number().optional(),
  dbScansByQuery: rowCountsBySourceSchema.optional(),
  joinPlans: v.array(planDebugEventJSONSchema).optional(),
});

export type AnalyzeQueryResult = v.Infer<typeof analyzeQueryResultSchema>;
