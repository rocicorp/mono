import {declaredParams} from 'firebase-functions/params';
import assert from 'node:assert';

assert(process.env.NODE_ENV === 'test', 'Only import this file in tests');

export function mockCloudflareStringParam() {
  for (const p of declaredParams) {
    if (p.name === 'CLOUDFLARE_ACCOUNT_ID') {
      p.value = p.toString = () => 'default-cloudflare-id';
    }
  }
}
