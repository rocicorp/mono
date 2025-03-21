/* eslint-disable @typescript-eslint/naming-convention */

export const CRUD = 'crud';
export const Custom = 'custom';
export type MutationType = typeof CRUD | typeof Custom;

export type CRUD = typeof CRUD;
export type Custom = typeof Custom;
