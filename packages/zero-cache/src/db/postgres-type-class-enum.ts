/* eslint-disable @typescript-eslint/naming-convention */

// Values of the `typtype` column in https://www.postgresql.org/docs/17/catalog-pg-type.html#CATALOG-PG-TYPE

export const Base = 'b';
export const Composite = 'c';
export const Domain = 'd';
export const Enum = 'e';
export const Pseudo = 'p';
export const Range = 'r';
export const Multirange = 'm';

export type Base = typeof Base;
export type Composite = typeof Composite;
export type Domain = typeof Domain;
export type Enum = typeof Enum;
export type Pseudo = typeof Pseudo;
export type Range = typeof Range;
export type Multirange = typeof Multirange;
