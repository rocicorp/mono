import * as v from '../../shared/src/valita.ts';
import {conditionSchema} from '../../zero-protocol/src/ast.ts';

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
      postMutation: policySchema.optional(),
    })
    .optional(),
  delete: policySchema.optional(),
});

export type AssetPermissions = v.Infer<typeof assetSchema>;

const tablePermissionsSchema = v.record(
  v.object({
    row: assetSchema.optional(),
    cell: v.record(assetSchema).optional(),
  }),
);

export const permissionsConfigSchema = v.object({
  tables: tablePermissionsSchema.optional(),
});

export type TablePermissions = v.Infer<typeof tablePermissionsSchema>;
export type PermissionsConfig = v.Infer<typeof permissionsConfigSchema>;
