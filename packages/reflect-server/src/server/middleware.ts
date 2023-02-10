import type { Obj, Router } from "itty-router";

// This type satisfies the itty-router Request type and the CF Request type.
// The CF Request is a class so it's not easy to intersect with itty's Request
// which is an interface. In consultation with typescript nerds we concluded that
// this is a good way of defining the type. The primary alternative would be to
// do a dynamic thing that pulls properties and methods out of the CF Request
// class and crates a new type dynamically.
export type RociRequest = {
  // From itty-router Request.
  params?: Obj;
  query?: Obj;

  // From CF Request.
  // NOTE that clone() returns a CF Request, not a RociRequest.
  clone(): Request;
  readonly method: string;
  readonly url: string;
  readonly headers: Headers;
  readonly redirect: string;
  readonly fetcher: Fetcher | null;
  readonly signal: AbortSignal;
  readonly cf?: IncomingRequestCfProperties;
  readonly body: ReadableStream | null;
  readonly bodyUsed: boolean;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
  json<T>(): Promise<T>;
  formData(): Promise<FormData>;
  blob(): Promise<Blob>;
};

export type RociRouter = Router<RociRequest>;

type Env = {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  REFLECT_AUTH_API_KEY?: string;
};

// Middleware that requires the auth API key to match an argument. This is
// used in the authDO which does not have access to the env.
export function requireAuthAPIKeyMatches(authApiKey: string | undefined) {
  return (request: RociRequest) => {
    return requireAuthAPIKeyMatchesEnv(request, {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      REFLECT_AUTH_API_KEY: authApiKey,
    });
  };
}

// Middlware that requires the auth API key in the env. This is used in the
// worker, which gets the key directly from the env.
export function requireAuthAPIKeyMatchesEnv(
  request: RociRequest,
  env: Env
): Response | undefined {
  const authHeader = request.headers.get("x-reflect-auth-api-key");
  if (authHeader === undefined || env.REFLECT_AUTH_API_KEY === undefined) {
    return newUnauthorizedResponse();
  }
  if (authHeader !== env.REFLECT_AUTH_API_KEY) {
    return newUnauthorizedResponse();
  }
  // Returning undefined is part of the itty contract: it says proceed in
  // processing the request by invoking the next handler.
  return undefined;
}

function newUnauthorizedResponse(msg = "Unauthorized") {
  return new Response(msg, {
    status: 401,
  });
}
