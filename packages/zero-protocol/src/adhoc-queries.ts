import * as v from '../../shared/src/valita.ts';
import {astSchema, type AST} from './ast.ts';

/**
 * Reserved query name for ad-hoc queries.
 * Mirrors the pattern of CRUD_MUTATION_NAME = '_zero_crud' for mutations.
 */
export const ADHOC_QUERY_NAME = '_zero_adhoc';

/**
 * Args schema for ad-hoc queries.
 * The args contain the AST to be executed.
 */
export const adhocQueryArgSchema = v.object({
  ast: astSchema,
});

export type AdhocQueryArg = v.Infer<typeof adhocQueryArgSchema>;

/**
 * Type guard to check if a query name is the ad-hoc query name.
 */
export function isAdhocQueryName(name: string): boolean {
  return name === ADHOC_QUERY_NAME;
}

export type {AST};
