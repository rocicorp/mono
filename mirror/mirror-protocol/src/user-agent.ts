import * as v from 'shared/src/valita.js';

export const userAgentSchema = v.object({
  type: v.string(),
  version: v.string(),
});
export type UserAgent = v.Infer<typeof userAgentSchema>;
