/**
 * URLPattern types for Node.js 22+
 * URLPattern is available globally in Node.js 22.0.0+ but types are not yet in @types/node
 */

interface URLPatternInit {
  protocol?: string | undefined;
  username?: string | undefined;
  password?: string | undefined;
  hostname?: string | undefined;
  port?: string | undefined;
  pathname?: string | undefined;
  search?: string | undefined;
  hash?: string | undefined;
  baseURL?: string | undefined;
}

interface URLPatternComponentResult {
  input: string;
  groups: Record<string, string | undefined>;
}

interface URLPatternResult {
  inputs: [URLPatternInit] | [URLPatternInit, string];
  protocol: URLPatternComponentResult;
  username: URLPatternComponentResult;
  password: URLPatternComponentResult;
  hostname: URLPatternComponentResult;
  port: URLPatternComponentResult;
  pathname: URLPatternComponentResult;
  search: URLPatternComponentResult;
  hash: URLPatternComponentResult;
}

declare class URLPattern {
  constructor(init?: URLPatternInit | string, baseURL?: string);

  test(input?: URLPatternInit | string, baseURL?: string): boolean;
  exec(
    input?: URLPatternInit | string,
    baseURL?: string,
  ): URLPatternResult | null;

  readonly protocol: string;
  readonly username: string;
  readonly password: string;
  readonly hostname: string;
  readonly port: string;
  readonly pathname: string;
  readonly search: string;
  readonly hash: string;
}
