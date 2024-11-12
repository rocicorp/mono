import * as v from '../../shared/src/valita.js';
import {astSchema} from '../../zero-protocol/src/ast.js';

const ruleSchema = v.tuple([v.literal('allow'), astSchema]);
export type Rule = v.Infer<typeof ruleSchema>;
const policySchema = v.array(ruleSchema);
export type Policy = v.Infer<typeof policySchema>;

const assetSchema = v.object({
  select: policySchema.optional(),
  insert: policySchema.optional(),
  update: policySchema.optional(),
  delete: policySchema.optional(),
});

export type AssetAuthorization = v.Infer<typeof assetSchema>;

const authorizationConfigSchema = v.record(
  v.object({
    table: assetSchema.optional(),
    column: v.record(assetSchema).optional(),
    row: assetSchema.optional(),
    cell: v.record(assetSchema).optional(),
  }),
);

export type AuthorizationConfig = v.Infer<typeof authorizationConfigSchema>;
