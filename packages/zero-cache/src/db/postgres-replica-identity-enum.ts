/* eslint-disable @typescript-eslint/naming-convention */

// Values of the `relreplident` column in https://www.postgresql.org/docs/current/catalog-pg-class.html

export const Default = 'd';
export const Nothing = 'n';
export const Full = 'f';
export const Index = 'i';

export type Default = typeof Default;
export type Nothing = typeof Nothing;
export type Full = typeof Full;
export type Index = typeof Index;
