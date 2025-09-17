/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
/* eslint-disable @typescript-eslint/naming-convention */

export const SDD = 4;
export const DD31 = 5;
// V6 added refreshHashes and persistHash to Client to fix ChunkNotFound errors
export const V6 = 6;
// V7 added sizeOfEntry to the BTree chunk data.
export const V7 = 7;
export const Latest = V7;

export type SDD = typeof SDD;
export type DD31 = typeof DD31;
export type V6 = typeof V6;
export type V7 = typeof V7;
export type Latest = typeof Latest;
