import type {HTTPString} from './http-string.js';

function validateServerParam(paramName: string, server: string): HTTPString {
  const expectedProtocol = 'http';
  const forExample = () =>
    ` For example: "${expectedProtocol}s://myapp-myteam.zero.ms/".`;

  if (
    !server.startsWith(`${expectedProtocol}://`) &&
    !server.startsWith(`${expectedProtocol}s://`)
  ) {
    throw new Error(
      `ZeroOptions.${paramName} must use the "${expectedProtocol}" or "${expectedProtocol}s" scheme.`,
    );
  }
  let url;
  try {
    url = new URL(server);
  } catch {
    throw new Error(
      `ZeroOptions.${paramName} must be a valid URL.${forExample()}`,
    );
  }

  const urlString = url.toString();

  if (url.pathname !== '/') {
    throw new Error(
      `ZeroOptions.${paramName} must not contain a path component (other than "/").${forExample()}`,
    );
  }

  for (const [property, invalidEndsWith] of [
    ['search', '?'],
    ['hash', '#'],
  ] as const) {
    if (url[property] || urlString.endsWith(invalidEndsWith)) {
      throw new Error(
        `ZeroOptions.${paramName} must not contain a ${property} component.${forExample()}`,
      );
    }
  }

  return urlString as HTTPString;
}

export function getServer(
  server: string | undefined | null,
): HTTPString | null {
  if (server === undefined || server === null) {
    return null;
  }
  return validateServerParam('server', server);
}
