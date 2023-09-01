import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import * as path from './path.js';

export const cloudflareSchema = v.object({
  // The domain on which workers are hosted.
  domain: v.string(),
  defaultMaxApps: v.number(),
});

export type Cloudflare = v.Infer<typeof cloudflareSchema>;

export const cloudflareDataConverter = firestoreDataConverter(cloudflareSchema);

export const CLOUDFLARE_COLLECTION = 'cloudflares';

export function cloudflarePath(cfID: string): string {
  return path.join(CLOUDFLARE_COLLECTION, cfID);
}
