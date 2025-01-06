export function string<T extends string = string>(): {
  type: 'string';
  optional: false;
  customType: T;
};
export function string<T extends string = string>(
  optional: false,
): {
  type: 'string';
  optional: false;
  customType: T;
};
export function string<T extends string = string>(
  optional: true,
): {
  type: 'string';
  optional: true;
  customType: T;
};
export function string<T extends string = string>(optional?: boolean) {
  return {
    type: 'string',
    optional: optional ?? false,
    customType: null as unknown as T,
  } as const;
}

export function number<T extends number = number>(): {
  type: 'number';
  optional: false;
  customType: T;
};
export function number<T extends number = number>(
  optional: false,
): {
  type: 'number';
  optional: false;
  customType: T;
};
export function number<T extends number = number>(
  optional: true,
): {
  type: 'number';
  optional: true;
  customType: T;
};
export function number<T extends number = number>(optional?: boolean) {
  return {
    type: 'number',
    optional: optional ?? false,
    customType: null as unknown as T,
  } as const;
}

export function boolean<T extends boolean = boolean>(): {
  type: 'boolean';
  optional: false;
  customType: T;
};
export function boolean<T extends boolean = boolean>(
  optional: false,
): {
  type: 'boolean';
  optional: false;
  customType: T;
};
export function boolean<T extends boolean = boolean>(
  optional: true,
): {
  type: 'boolean';
  optional: true;
  customType: T;
};
export function boolean<T extends boolean = boolean>(optional?: boolean) {
  return {
    type: 'boolean',
    optional: optional ?? false,
    customType: null as unknown as T,
  } as const;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function json<T = any>(): {
  type: 'json';
  optional: false;
  customType: T;
};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function json<T = any>(
  optional: false,
): {
  type: 'json';
  optional: false;
  customType: T;
};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function json<T = any>(
  optional: true,
): {
  type: 'json';
  optional: true;
  customType: T;
};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function json<T = any>(optional?: boolean) {
  return {
    type: 'json',
    optional: optional ?? false,
    customType: null as unknown as T,
  } as const;
}

export function enumeration<T extends string>(): {
  type: 'string';
  kind: 'enum';
  optional: false;
  customType: T;
};
export function enumeration<T extends string>(
  optional: false,
): {
  type: 'string';
  kind: 'enum';
  optional: false;
  customType: T;
};
export function enumeration<T extends string>(
  optional: true,
): {
  type: 'string';
  kind: 'enum';
  optional: true;
  customType: T;
};
export function enumeration<T extends string>(optional?: boolean) {
  return {
    type: 'string',
    kind: 'enum',
    customType: null as unknown as T,
    optional: optional ?? false,
  } as const;
}
