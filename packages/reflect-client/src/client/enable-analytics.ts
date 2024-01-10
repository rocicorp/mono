// https://www.oreilly.com/library/view/regular-expressions-cookbook/9780596802837/ch07s16.html
const IPV4_ADDRESS_REGEX =
  /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;
// This doesn't ensure a valid ipv6, but any ipv6 hostname will
// match this regex, and no domain based hostnames will.
const IPV6_ADDRESS_HOSTNAME_REGEX = /^\[[a-fA-F0-9:]*:[a-fA-F0-9:]*\]$/;

export const IP_ADDRESS_HOSTNAME_REGEX = new RegExp(
  `(${IPV4_ADDRESS_REGEX.source}|${IPV6_ADDRESS_HOSTNAME_REGEX.source})`,
);

export function shouldEnableAnalytics(options: {
  server: string | null;
  enableAnalytics: boolean | undefined;
}): boolean {
  const {server, enableAnalytics = true} = options;
  if (!enableAnalytics) {
    return false;
  }
  const serverURL = server === null ? null : new URL(server);
  const socketHostname = serverURL?.hostname;
  // If the hostname is undefined, localhost, or an ip address, then
  // this is most likely a test or local development, in which case we
  // do not want to enable analytics.
  return (
    server !== null &&
    socketHostname !== undefined &&
    socketHostname !== 'localhost' &&
    !IP_ADDRESS_HOSTNAME_REGEX.test(socketHostname)
  );
}
