export function string<T extends string = string>(optional: boolean = false) {
  return {
    type: 'string',
    optional,
    customType: null as unknown as T,
  } as const;
}

export function number<T extends number = number>(optional: boolean = false) {
  return {
    type: 'number',
    optional,
    customType: null as unknown as T,
  } as const;
}

export function boolean<T extends boolean = boolean>(
  optional: boolean = false,
) {
  return {
    type: 'boolean',
    optional,
    customType: null as unknown as T,
  } as const;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function json<T = any>(optional: boolean = false) {
  return {
    type: 'json',
    optional,
    customType: null as unknown as T,
  } as const;
}

export function enumeration<T extends string>(optional: boolean = false) {
  return {
    type: 'string',
    kind: 'enum',
    customType: null as unknown as T,
    optional,
  } as const;
}
