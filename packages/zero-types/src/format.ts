/**
 * Format descriptor for query results.
 * Describes whether a result should be singular or a list,
 * and what the format of nested relationships should be.
 */
export type Format = {
  singular: boolean;
  relationships: Record<string, Format>;
};

export const defaultFormat: Format = {
  singular: false,
  relationships: {},
} as const;
