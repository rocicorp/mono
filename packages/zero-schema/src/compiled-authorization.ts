import * as v from '../../shared/src/valita.js';
import {conditionSchema} from '../../zero-protocol/src/ast.js';

const ruleSchema = v.tuple([v.literal('allow'), conditionSchema]);
export type Rule = v.Infer<typeof ruleSchema>;
const policySchema = v.array(ruleSchema);
export type Policy = v.Infer<typeof policySchema>;

const assetSchema = v.object({
  select: policySchema.optional(),
  insert: policySchema.optional(),
  update: v
    .object({
      preMutation: policySchema.optional(),
      postProposedMutation: policySchema.optional(),
    })
    .optional(),
  delete: policySchema.optional(),
});

export type AssetAuthorization = v.Infer<typeof assetSchema>;

export const authorizationConfigSchema = v.record(
  v.object({
    row: assetSchema.optional(),
    cell: v.record(assetSchema).optional(),
  }),
);

export type AuthorizationConfig = v.Infer<typeof authorizationConfigSchema>;
