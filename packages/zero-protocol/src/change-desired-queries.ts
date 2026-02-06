import * as v from '../../shared/src/valita.ts';
import {upQueriesPatchSchema} from './queries-patch.ts';

const changeDesiredQueriesBodySchema = v.object({
  desiredQueriesPatch: upQueriesPatchSchema,
  auth: v.string().nullable().optional(),
});

export const changeDesiredQueriesMessageSchema = v.tuple([
  v.literal('changeDesiredQueries'),
  changeDesiredQueriesBodySchema,
]);

export type ChangeDesiredQueriesBody = v.Infer<
  typeof changeDesiredQueriesBodySchema
>;
export type ChangeDesiredQueriesMessage = v.Infer<
  typeof changeDesiredQueriesMessageSchema
>;
