/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
