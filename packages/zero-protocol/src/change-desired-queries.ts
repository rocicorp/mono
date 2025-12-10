import * as v from '../../shared/src/valita.ts';
import {desiredQueriesPatchSchema} from './queries-patch.ts';

const changeDesiredQueriesBodySchema = v.object({
  desiredQueriesPatch: desiredQueriesPatchSchema,
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
