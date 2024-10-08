import * as v from '../../shared/src/valita.js';
import {queriesPatchSchema} from './queries-patch.js';

const changeDesiredQueriesBodySchema = v.object({
  desiredQueriesPatch: queriesPatchSchema,
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
