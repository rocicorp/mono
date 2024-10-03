/* eslint-disable @typescript-eslint/naming-convention */

export const Applied = 0;
export const NoOp = 1;
export const CookieMismatch = 2;

export type Applied = typeof Applied;
export type NoOp = typeof NoOp;
export type CookieMismatch = typeof CookieMismatch;

export type Type = Applied | NoOp | CookieMismatch;
