/* eslint-disable @typescript-eslint/naming-convention */

export const InitialRun = 0;
export const Regular = 1;

export type InitialRun = typeof InitialRun;
export type Regular = typeof Regular;

export type Type = InitialRun | Regular;
