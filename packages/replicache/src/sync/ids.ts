/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import * as valita from '../../../shared/src/valita.ts';

/**
 * The ID describing a group of clients. All clients in the same group share a
 * persistent storage (IDB).
 */
export type ClientGroupID = string;

export const clientGroupIDSchema: valita.Type<ClientGroupID> = valita.string();

/**
 * The ID describing a client.
 */
export type ClientID = string;

export const clientIDSchema: valita.Type<ClientID> = valita.string();
