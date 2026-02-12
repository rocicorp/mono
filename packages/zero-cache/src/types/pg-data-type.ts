import type {ValueType} from '../../../zero-protocol/src/client-schema.ts';

export const pgToZqlNumericTypeMap = Object.freeze({
  'smallint': 'number',
  'integer': 'number',
  'int': 'number',
  'int2': 'number',
  'int4': 'number',
  'int8': 'number',
  'bigint': 'number',
  'smallserial': 'number',
  'serial': 'number',
  'serial2': 'number',
  'serial4': 'number',
  'serial8': 'number',
  'bigserial': 'number',
  'decimal': 'number',
  'numeric': 'number',
  'real': 'number',
  'double precision': 'number',
  'float': 'number',
  'float4': 'number',
  'float8': 'number',
});

export function isPgNumberType(pgType: string): boolean {
  return Object.hasOwn(pgToZqlNumericTypeMap, formatTypeForLookup(pgType));
}

export const pgToZqlStringTypeMap = Object.freeze({
  'bpchar': 'string',
  'character': 'string',
  'character varying': 'string',
  'text': 'string',
  'uuid': 'string',
  'varchar': 'string',
});

export function isPgStringType(pgType: string): boolean {
  return Object.hasOwn(pgToZqlStringTypeMap, formatTypeForLookup(pgType));
}

export const pgToZqlTypeMap = Object.freeze({
  // Numeric types
  ...pgToZqlNumericTypeMap,

  // Date/Time types
  'date': 'number',
  'time': 'number',
  'time with time zone': 'number',
  'time without time zone': 'number',
  'timestamp': 'number',
  'timestamptz': 'number',
  'timestamp with time zone': 'number',
  'timestamp without time zone': 'number',

  // String types
  ...pgToZqlStringTypeMap,

  // Boolean types
  'bool': 'boolean',
  'boolean': 'boolean',

  'json': 'json',
  'jsonb': 'json',

  // TODO: Add support for these.
  // 'bytea':
});

export function dataTypeToZqlValueType(
  pgType: string,
  isEnum: boolean,
  isArray: boolean,
): ValueType | undefined {
  // We treat pg arrays as JSON values.
  if (isArray) {
    return 'json';
  }

  const valueType = (pgToZqlTypeMap as Record<string, ValueType>)[
    formatTypeForLookup(pgType)
  ];
  if (valueType === undefined && isEnum) {
    return 'string';
  }
  return valueType;
}

// Strips args (i.e. (32) in char(32)) and lowercases.
function formatTypeForLookup(pgType: string): string {
  const startOfArgs = pgType.indexOf('(');
  if (startOfArgs === -1) {
    return pgType.toLocaleLowerCase();
  }
  return pgType.toLocaleLowerCase().substring(0, startOfArgs);
}
