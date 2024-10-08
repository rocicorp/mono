import * as valita from '../../../shared/src/valita.js';

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
