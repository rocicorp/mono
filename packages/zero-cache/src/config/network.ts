import type {LogContext} from '@rocicorp/logger';
import {networkInterfaces} from 'os';

function isLinkLocal(addr: string) {
  return addr.startsWith('169.254.') || addr.startsWith('fe80::');
}

export function getHostIp(lc: LogContext) {
  const interfaces = networkInterfaces();
  const sorted = Object.values(networkInterfaces())
    .flat()
    .filter(val => val !== undefined)
    .sort((a, b) => {
      if (a.internal !== b.internal) {
        // Prefer non-internal addresses.
        return a.internal ? 1 : -1;
      }
      if (isLinkLocal(a.address) !== isLinkLocal(b.address)) {
        // Prefer non link-local addresses
        return isLinkLocal(a.address) ? 1 : -1;
      }
      if (a.family !== b.family) {
        // Prefer IPv4.
        return a.family === 'IPv4' ? -1 : 1;
      }
      // arbitrary
      return a.address.localeCompare(b.address);
    });

  const preferred = sorted[0].address;
  lc.info?.(`network interfaces`, {preferred, sorted, interfaces});
  return preferred;
}
