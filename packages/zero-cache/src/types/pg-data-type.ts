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

export const pgToZqlNativeStringTypeMap = Object.freeze({
  'bpchar': 'string',
  'character': 'string',
  'character varying': 'string',
  'text': 'string',
  'varchar': 'string',
});

export function isPgNativeStringType(pgType: string): boolean {
  return Object.hasOwn(pgToZqlNativeStringTypeMap, formatTypeForLookup(pgType));
}

export const pgToZqlTextRepresentedTypeMap = Object.freeze({
  cidr: 'string',
  ean13: 'string',
  inet: 'string',
  isbn: 'string',
  isbn13: 'string',
  ismn: 'string',
  ismn13: 'string',
  issn: 'string',
  issn13: 'string',
  macaddr: 'string',
  macaddr8: 'string',
  pg_lsn: 'string',
  upc: 'string',
  uuid: 'string',
});

export function isPgTextRepresentedType(pgType: string): boolean {
  return Object.hasOwn(
    pgToZqlTextRepresentedTypeMap,
    formatTypeForLookup(pgType),
  );
}

export const pgToZqlStringTypeMap = Object.freeze({
  ...pgToZqlNativeStringTypeMap,
  ...pgToZqlTextRepresentedTypeMap,
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
  'timetz': 'number',
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
