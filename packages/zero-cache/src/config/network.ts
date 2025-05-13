import type {LogContext} from '@rocicorp/logger';
import {networkInterfaces, type NetworkInterfaceInfo} from 'os';

function isLinkLocal(addr: string) {
  return addr.startsWith('169.254.') || addr.startsWith('fe80::');
}

export function getHostIp(lc: LogContext, preferredPrefixes: string[]) {
  const interfaces = networkInterfaces();
  const preferred = getPreferredIp(interfaces, preferredPrefixes);
  lc.info?.(`network interfaces`, {preferred, interfaces});
  return preferred;
}

export function getPreferredIp(
  interfaces: NodeJS.Dict<NetworkInterfaceInfo[]>,
  preferredPrefixes: string[],
) {
  const rank = ({name}: {name: string}) => {
    for (let i = 0; i < preferredPrefixes.length; i++) {
      if (name.startsWith(preferredPrefixes[i])) {
        return i;
      }
    }
    return Number.MAX_SAFE_INTEGER;
  };

  const sorted = Object.entries(interfaces)
    .map(([name, infos]) =>
      (infos ?? []).map(info => ({
        ...info,
        // Enclose IPv6 addresses in square brackets for use in a URL.
        address: info.family === 'IPv4' ? info.address : `[${info.address}]`,
        name,
      })),
    )
    .flat()
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
      const rankA = rank(a);
      const rankB = rank(b);
      if (rankA !== rankB) {
        return rankA - rankB;
      }
      // arbitrary
      return a.address.localeCompare(b.address);
    });

  const preferred = sorted[0].address;
  return preferred;
}
