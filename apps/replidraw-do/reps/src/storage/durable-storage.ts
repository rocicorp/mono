import { JSONValue } from "replicache";
import { delEntry, getEntry, putEntry } from "../db/data";
import type { Storage } from "./storage";
import { ZodSchema } from "zod";

/**
 * Implements the Storage interface in terms of the database.
 */
export class DurableStorage implements Storage {
  private _durable: DurableObjectStorage;

  constructor(durable: DurableObjectStorage) {
    this._durable = durable;
  }

  async put<T extends JSONValue>(key: string, value: T): Promise<void> {
    return putEntry(this._durable, key, value);
  }
  async del(key: string): Promise<void> {
    return delEntry(this._durable, key);
  }
  async get<T extends JSONValue>(
    key: string,
    schema: ZodSchema<T>
  ): Promise<T | undefined> {
    return await getEntry(this._durable, key, schema);
  }
}
