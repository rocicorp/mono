import type {HTTPString} from './http-string.js';

function validateServerParam(paramName: string, server: string): HTTPString {
  const expectedProtocol = 'http';
  const forExample = (path: string = '') =>
    ` For example: "${expectedProtocol}s://myapp-myteam.zero.ms/${path}".`;

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

  const pathComponents = url.pathname.split('/');
  if (pathComponents[0] === '') {
    pathComponents.shift();
  }
  if (pathComponents[pathComponents.length - 1] === '') {
    pathComponents.pop();
  }
  if (pathComponents.length > 1) {
    throw new Error(
      `ZeroOptions.${paramName} may have at most one path component.${forExample(
        'zero',
      )}`,
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
    console.warn(
      'Zero starting up with no server URL. This is supported for unit testing ' +
        'and prototyping, but no data will be synced.',
    );
    return null;
  }
  return validateServerParam('server', server);
}
