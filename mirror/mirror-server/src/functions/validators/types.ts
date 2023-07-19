import type {CallableRequest} from 'firebase-functions/v2/https';
import type {AuthData} from 'firebase-functions/v2/tasks';
import type {App} from 'mirror-schema/src/app.js';
import type {Role} from 'mirror-schema/src/membership.js';
import type {User} from 'mirror-schema/src/user.js';

export type AsyncCallable<Request, Response> = (
  request: CallableRequest<Request>,
) => Promise<Response>;

export type AsyncHandler<Request, Response> = (
  request: Request,
  context: CallableRequest<Request>,
) => Promise<Response>;

export type AsyncHandlerWithAuth<Request, Response> = (
  request: Request,
  context: CallableRequestWithAuth<Request>,
) => Promise<Response>;

export type AsyncAppHandler<Request, Response> = (
  request: Request,
  context: CallableRequestWithAppAuth<Request>,
) => Promise<Response>;

export interface CallableRequestWithAuth<Request>
  extends CallableRequest<Request> {
  auth: AuthData;
}

export type AppAuthorization = {
  app: App;
  user: User;
  role: Role;
};

export interface CallableRequestWithAppAuth<Request>
  extends CallableRequestWithAuth<Request> {
  authorized: AppAuthorization;
}
