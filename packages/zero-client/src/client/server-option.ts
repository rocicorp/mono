import {WSString, toHTTPString, type HTTPString} from './http-string.js';

function validateServerParam<
  S extends 'ws' | 'http',
  R = S extends 'ws' ? WSString : HTTPString,
>(paramName: string, server: string, expectedProtocol: S): R {
  const forExample = () =>
    ` For example: "${expectedProtocol}s://myapp-myteam.reflect.net/".`;

  if (
    !server.startsWith(`${expectedProtocol}://`) &&
    !server.startsWith(`${expectedProtocol}s://`)
  ) {
    throw new Error(
      `ReflectOptions.${paramName} must use the "${expectedProtocol}" or "${expectedProtocol}s" scheme.`,
    );
  }
  let url;
  try {
    url = new URL(server);
  } catch {
    throw new Error(
      `ReflectOptions.${paramName} must be a valid URL.${forExample()}`,
    );
  }

  const urlString = url.toString();

  if (url.pathname !== '/') {
    throw new Error(
      `ReflectOptions.${paramName} must not contain a path component (other than "/").${forExample()}`,
    );
  }

  for (const [property, invalidEndsWith] of [
    ['search', '?'],
    ['hash', '#'],
  ] as const) {
    if (url[property] || urlString.endsWith(invalidEndsWith)) {
      throw new Error(
        `ReflectOptions.${paramName} must not contain a ${property} component.${forExample()}`,
      );
    }
  }

  return urlString as R;
}

export function getServer(
  server: string | null | undefined,
  socketOrigin: string | null | undefined,
): HTTPString | null {
  if (server) {
    return validateServerParam('server', server, 'http') as HTTPString;
  }

  if (socketOrigin) {
    const validatedSocketOrigin = validateServerParam(
      'socketOrigin',
      socketOrigin,
      'ws',
    );
    return toHTTPString(validatedSocketOrigin);
  }

  return null;
}
