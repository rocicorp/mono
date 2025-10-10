import * as expo from './expo-sqlite.ts';
import * as sqlite from './sqlite.ts';

// We reassign these so that we can mark them deprecated in the docs/IDEs.

/** @deprecated Use `expoSQLiteStoreProvider` from `@rocicorp/zero/expo-sqlite` instead. */
export const expoSQLiteStoreProvider = expo.expoSQLiteStoreProvider;

/** @deprecated Use `clearAllNamedStoresForTesting` from `@rocicorp/zero/expo-sqlite` instead. */
export const clearAllNamedStoresForTesting =
  sqlite.clearAllNamedStoresForTesting;

/** @deprecated Use `dropStore` from `@rocicorp/zero/expo-sqlite` instead. */
export const dropStore = sqlite.dropStore;

/** @deprecated Use `SQLiteStore` from `@rocicorp/zero/expo-sqlite` instead. */
export const SQLiteStore = sqlite.SQLiteStore;

/** @deprecated Use `ExpoSQLiteStoreOptions` from `@rocicorp/zero/expo-sqlite` instead. */
export type ExpoSQLiteStoreOptions = expo.ExpoSQLiteStoreOptions;

/** @deprecated Use `PreparedStatement` from `@rocicorp/zero/expo-sqlite` instead. */
export type PreparedStatement = sqlite.PreparedStatement;

/** @deprecated Use `SQLiteDatabase` from `@rocicorp/zero/expo-sqlite` instead. */
export type SQLiteDatabase = sqlite.SQLiteDatabase;

/** @deprecated Use `SQLiteStoreOptions` from `@rocicorp/zero/expo-sqlite` instead. */
export type SQLiteStoreOptions = sqlite.SQLiteStoreOptions;

/** @deprecated Use `CreateStore` from `@rocicorp/zero/expo-sqlite` instead. */
export type CreateStore = sqlite.CreateStore;

/** @deprecated Use `DropStore` from `@rocicorp/zero/expo-sqlite` instead. */
export type DropStore = sqlite.DropStore;

/** @deprecated Use `Read` from `@rocicorp/zero/expo-sqlite` instead. */
export type Read = sqlite.Read;

/** @deprecated Use `Store` from `@rocicorp/zero/expo-sqlite` instead. */
export type Store = sqlite.Store;

/** @deprecated Use `StoreProvider` from `@rocicorp/zero/expo-sqlite` instead. */
export type StoreProvider = sqlite.StoreProvider;

/** @deprecated Use `Write` from `@rocicorp/zero/expo-sqlite` instead. */
export type Write = sqlite.Write;
