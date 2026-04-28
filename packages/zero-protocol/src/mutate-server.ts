import * as v from '../../shared/src/valita.ts';
import {pushFailedBodySchema} from './error.ts';
import {mutationResponseSchema} from './mutation.ts';
import {pushErrorSchema} from './push.ts';

const legacyPushSuccessSchema = v.object({
  mutations: v.array(mutationResponseSchema),
});

const legacyPushResponseSchema = v.union(
  legacyPushSuccessSchema,
  pushErrorSchema,
  pushFailedBodySchema,
);

export const mutateSuccessSchema = v.object({
  kind: v.literal('MutateResponse'),
  userID: v.string().nullable().optional(),
  mutations: v.array(mutationResponseSchema),
});
export type MutateSuccess = v.Infer<typeof mutateSuccessSchema>;

export const mutateResponseSchema = v.union(
  mutateSuccessSchema,
  pushFailedBodySchema,
  // for backwards compatibility
  legacyPushResponseSchema,
);
export type MutateResponse = v.Infer<typeof mutateResponseSchema>;

/**
 * The schema for the querystring parameters of the custom mutate endpoint.
 */
export const mutateParamsSchema = v.object({
  schema: v.string(),
  appID: v.string(),
});
