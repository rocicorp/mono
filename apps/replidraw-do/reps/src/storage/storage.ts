import type { JSONValue } from "replicache";
import { ZodSchema } from "zod";

/**
 * Abstract storage interface used throughout the server for storing both user
 * and system data.
 */
export interface Storage {
  put<T extends JSONValue>(key: string, value: T): Promise<void>;
  del(key: string): Promise<void>;
  get<T extends JSONValue>(
    key: string,
    schema: ZodSchema<T>
  ): Promise<T | undefined>;
  // TODO: support for scanning.
}
