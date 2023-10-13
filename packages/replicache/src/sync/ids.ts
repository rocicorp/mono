import * as valita from 'shared/src/valita.js';

/**
 * The ID describing a group of clients. All clients in the same group share a
 * persistent storage (IDB).
 */
export type ClientGroupID = string;

export const clientGroupIDSchema: valita.ValitaType<ClientGroupID> =
  valita.string();

/**
 * The ID describing a client.
 */
export type ClientID = string;

export const clientIDSchema: valita.ValitaType<ClientID> = valita.string();
