import {declaredParams} from 'firebase-functions/params';

export function mockCloudflareStringParam() {
  for (const p of declaredParams) {
    if (p.name === 'CLOUDFLARE_ACCOUNT_ID') {
      p.value = p.toString = () => 'default-cloudflare-id';
    }
  }
}
