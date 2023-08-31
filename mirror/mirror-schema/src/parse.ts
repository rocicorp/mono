import * as v from 'shared/src/valita.js';

// Firestore DocumentData-friendly version of valita.parse().
export function parse<T extends Record<string, unknown>>(
  value: unknown,
  schema: v.Type<T>,
  mode?: v.ParseOptionsMode,
): T {
  const parsed = v.parse(value, schema, mode);
  return ensurePlainObject(parsed) as T;
}

// valita parsing for schemas with fields that have default values can result
// in the creation of objects that violate assumptions of the Firestore libraries;
// namely, that all Objects have constructors. This fixes it.
function ensurePlainObject(
  value: Record<string, unknown>,
): Record<string, unknown> {
  if (isMissingConstructor(value)) {
    // Shallow copy to ensure this will pass firestore isPlainObject check, which requires objects to
    // have a constructor with name 'Object'. valita creates objects without a prototype and thus
    // without a constructor
    // https://github.com/badrap/valita/blob/5db630edb1397959f613b94b0f9e22ceb8ec78d4/src/index.ts#L568
    value = {...value};
  }
  for (const [name, val] of Object.entries(value)) {
    if (isObject(val)) {
      value[name] = ensurePlainObject(val);
    }
  }
  return value;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return Object.prototype.toString.call(value) === '[object Object]';
}

function isMissingConstructor(input: unknown): boolean {
  return isObject(input) && input.constructor === undefined;
}
