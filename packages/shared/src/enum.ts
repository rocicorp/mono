/**
 *
 * The way to transition old TS enums to non TS enums is to do:
 *
 * ```
 * const enum E = {
 *   A = 1,
 *   B = 2,
 *   D = 4,
 * }
 * ```
 *
 * ```
 * // e-enum.ts
 * export const A = 1;
 * export const B = 2;
 * export const D = 4;
 *
 * export type A = typeof A;
 * export type B = typeof B;
 * export type D = typeof D;
 * ```
 *
 * Then in the importer do:
 *
 * ```
 * import * as E from './e-enum.ts';
 * import type {Enum} from 'shared/enum.ts';
 *
 * type E = Enum<typeof E>;
 * ```
 *
 * Then you can use E and E.A as both a type and a value.
 *
 * https://esbuild.github.io/try/#YgAwLjI0LjIALS1taW5pZnkgLS1idW5kbGUgLS1mb3JtYXQ9ZXNtAGUAZW50cnkuanMAaW1wb3J0ICogYXMgRSBmcm9tICcuL2UtZW51bS50cyc7CgppZiAoRS5BID09PSAxKSB7CiAgY29uc29sZS5sb2coRS5CKTsKfSBlbHNlIHsKICBjb25zb2xlLmxvZygndW5yZWFjaGFibGUnKTsKfQAAZS1lbnVtLnRzAGV4cG9ydCBjb25zdCBBID0gMTsKZXhwb3J0IGNvbnN0IEIgPSAyOwpleHBvcnQgY29uc3QgRCA9IDQ7CiAKZXhwb3J0IHR5cGUgQSA9IHR5cGVvZiBBOwpleHBvcnQgdHlwZSBCID0gdHlwZW9mIEI7CmV4cG9ydCB0eXBlIEQgPSB0eXBlb2YgRDs
 */
export type Enum<T> = T[keyof T];
