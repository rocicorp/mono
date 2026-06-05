/**
 * Format descriptor for query results.
 * Describes whether a result should be singular or a list,
 * and what the format of nested relationships should be.
 */
export type Format = {
  singular: boolean;
  relationships: Record<string, Format>;
  /**
   * When set, this relationship is a scalar aggregate (count/sum/avg): it
   * materializes as the single aggregate value (see zql ivm/aggregate.ts)
   * rather than the child rows. Implies `singular: true`. The descriptor (fn +
   * optional field) lets the query builder reconstruct the AST; the view only
   * checks its presence. (`fn` mirrors zero-protocol's `AggregateFunction`;
   * inlined to avoid a cross-package dependency.)
   */
  aggregate?:
    | {
        readonly fn: 'count' | 'sum' | 'avg' | 'min' | 'max';
        readonly field?: string | undefined;
      }
    | undefined;
};

export const defaultFormat: Format = {
  singular: false,
  relationships: {},
} as const;
