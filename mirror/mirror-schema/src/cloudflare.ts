import * as v from 'shared/valita.js';
import * as path from './path.js';

export const cloudflareSchema = v.object({
  name: v.string(),
  authToken: v.string(),
});

export type Cloudflare = v.Infer<typeof cloudflareSchema>;

export const CLOUDFLARE_COLLECTION = 'cloudflares';

export function cloudflarePath(cfID: string): string {
  return path.join(CLOUDFLARE_COLLECTION, cfID);
}
