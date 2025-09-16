/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import * as valita from '../../shared/src/valita.ts';

/**
 * The definition of a single index.
 */
export type IndexDefinition = {
  /**
   * The prefix, if any, to limit the index over. If not provided the values of
   * all keys are indexed.
   */
  readonly prefix?: string | undefined;

  /**
   * A [JSON Pointer](https://tools.ietf.org/html/rfc6901) pointing at the sub
   * value inside each value to index over.
   *
   * For example, one might index over users' ages like so:
   * `{prefix: '/user/', jsonPointer: '/age'}`
   */
  readonly jsonPointer: string;

  /**
   * If `true`, indexing empty values will not emit a warning.  Defaults to `false`.
   */
  readonly allowEmpty?: boolean | undefined;
};

export const indexDefinitionSchema: valita.Type<IndexDefinition> =
  valita.readonlyObject({
    prefix: valita.string().optional(),
    jsonPointer: valita.string(),
    allowEmpty: valita.boolean().optional(),
  });

/**
 * An object as a map defining the indexes. The keys are the index names and the
 * values are the index definitions.
 */
export type IndexDefinitions = {readonly [name: string]: IndexDefinition};

export const indexDefinitionsSchema = valita.readonlyRecord(
  indexDefinitionSchema,
);

export function indexDefinitionEqual(
  a: IndexDefinition,
  b: IndexDefinition,
): boolean {
  return (
    a.jsonPointer === b.jsonPointer &&
    (a.allowEmpty ?? false) === (b.allowEmpty ?? false) &&
    (a.prefix ?? '') === (b.prefix ?? '')
  );
}

export function indexDefinitionsEqual(
  a: IndexDefinitions,
  b: IndexDefinitions,
): boolean {
  if (Object.keys(a).length !== Object.keys(b).length) {
    return false;
  }
  for (const [aKey, aValue] of Object.entries(a)) {
    const bValue = b[aKey];
    if (!bValue || !indexDefinitionEqual(aValue, bValue)) {
      return false;
    }
  }
  return true;
}

export function assertIndexDefinitions(
  value: unknown,
): asserts value is IndexDefinitions {
  valita.assert(value, indexDefinitionsSchema);
}
