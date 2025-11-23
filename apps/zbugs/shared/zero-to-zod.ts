import {z} from 'zod/mini';
import type {TableSchema} from '../../../packages/zero-types/src/schema.ts';

const zodTypeMap = {
  string: z.string,
  number: z.number,
  boolean: z.boolean,
  json: z.any,
  null: z.null,
} as const;

type ZodShapeFromColumns<T extends TableSchema['columns']> = {
  [K in keyof T]: T[K]['optional'] extends true
    ? ReturnType<
        typeof z.optional<ReturnType<(typeof zodTypeMap)[T[K]['type']]>>
      >
    : ReturnType<(typeof zodTypeMap)[T[K]['type']]>;
};

export type ZeroToZodType<T extends TableSchema> = ReturnType<
  typeof z.object<ZodShapeFromColumns<T['columns']>>
>;

/**
 * Converts a Zero table schema to a Zod schema.
 * @param tableSchema - The Zero table schema to convert
 * @returns A Zod object schema representing the table columns
 *
 * @example
 * ```ts
 * // Convert the issue table schema to Zod
 * const issueZodSchema = zeroToZod(schema.tables.issue);
 * type IssueType = z.infer<typeof issueZodSchema>;
 *
 * // Use it with defineMutator
 * const myMutator = defineMutator(
 *   zeroToZod(schema.tables.issue),
 *   async ({tx, args, ctx}) => {
 *     // args will be typed based on the issue table columns
 *   }
 * );
 * ```
 */
export function zeroToZod<T extends TableSchema>(
  tableSchema: T,
): ZeroToZodType<T> {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const shape: Record<string, any> = {};

  for (const [columnName, columnSchema] of Object.entries(
    tableSchema.columns,
  )) {
    const zodFn = zodTypeMap[columnSchema.type];
    if (!zodFn) {
      throw new Error(`Unsupported column type: ${columnSchema.type}`);
    }

    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    let zodType: any = zodFn();

    if (columnSchema.optional) {
      zodType = z.optional(zodType);
    }

    shape[columnName] = zodType;
  }

  return z.object(shape) as ZeroToZodType<T>;
}
