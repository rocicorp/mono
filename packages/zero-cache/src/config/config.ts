import type {OptionalLogger} from '@rocicorp/logger';
import type {OptionDefinition} from 'command-line-args';
import commandLineArgs from 'command-line-args';
import commandLineUsage from 'command-line-usage';
import camelCase from 'lodash.camelcase';
import merge from 'lodash.merge';
import snakeCase from 'lodash.snakecase';
import {must} from '../../../shared/src/must.js';
import * as v from '../../../shared/src/valita.js';

type Primitive = number | string | boolean;
type Value = Primitive | Array<Primitive>;
type OptionType<T extends Value> = v.Type<T> | v.Optional<T>;

export type Option<T extends Value> =
  | OptionType<T>
  | {
      type: OptionType<T>;
      desc?: string[];
    };

// Related Options can be grouped.
type Group = Record<string, Option<Value>>;

export type Options = Record<string, Group | Option<Value>>;

/** Unwrap the Value type from an Option<V>. */
type ValueOf<T extends Option<Value>> = T extends v.Optional<infer V>
  ? V | undefined
  : T extends v.Type<infer V>
  ? V
  : T extends {type: OptionType<Value>}
  ? ValueOf<T['type']>
  : never;

/**
 * A Config is an object containing fields mapped to primitive values
 * or arrays of primitive values, and groups thereof (i.e. one level of grouping).
 *
 * Example:
 *
 * ```ts
 * {
 *   port: number;
 *
 *   changeStreamerConnStr: string;
 *
 *   // The "log" group
 *   log: {
 *     level: 'debug' | 'info' | 'warn' | 'error';
 *     format: 'text' | 'json'
 *   };
 *   ...
 * }
 * ```
 */
export type Config<O extends Options> = {
  [P in keyof O]: O[P] extends Option<Value>
    ? ValueOf<O[P]>
    : // O[P] is a Group
      {
        [K in keyof O[P]]: O[P][K] extends Option<Value>
          ? ValueOf<O[P][K]>
          : never;
      };
};

/**
 * Converts an Options instance into its corresponding {@link Config} schema.
 */
function configSchema<T extends Options>(options: T): v.Type<Config<T>> {
  function makeObjectType(options: Options | Group) {
    return v.object(
      Object.fromEntries(
        Object.entries(options).map(
          ([name, value]): [string, OptionType<Value> | v.Type<unknown>] => {
            if (v.instanceOfAbstractType(value)) {
              return [name, value];
            }
            const {type} = value;
            if (v.instanceOfAbstractType(type)) {
              return [name, type];
            }
            // OptionGroup
            return [name, makeObjectType(value as Group)];
          },
        ),
      ),
    );
  }
  return makeObjectType(options) as v.Type<Config<T>>;
}

export function parseOptions<T extends Options>(
  options: T,
  argv: string[],
  processEnv = process.env,
  logger: OptionalLogger = console,
): Config<T> {
  // The main logic for converting a valita Type spec to an Option (i.e. flag) spec.
  function addOption(name: string, option: Option<Value>, group?: string) {
    // The group name is prepended to the flag name and stripped in parseArgs().
    name = group ? camelCase(`${group}_${name}`) : name;

    const {type, desc = []} = 'name' in option ? {type: option} : option;

    const defaultResult = v.testOptional(undefined, type);
    const defaultValue = defaultResult.ok ? defaultResult.value : undefined;

    const literals: string[] = [];
    let multiple = type.name === 'array';
    let elemType = multiple ? (type as v.ArrayType).rest : type;
    let terminalType: string | undefined;

    type.toTerminals(t => {
      let typeName: string;
      switch (t.name) {
        case 'undefined':
        case 'optional':
          return;
        case 'array':
          multiple = true;
          elemType = must(t.rest);
          typeName = elemType.name;
          break;
        case 'literal':
          literals.push(String(t.value));
          typeName = typeof t.value;
          break;
        default:
          typeName = t.name;
          break;
      }
      if ((terminalType ??= typeName) !== typeName) {
        throw new TypeError(
          `--${name} flag has mixed types ${typeName} and ${terminalType}`,
        );
      }
    });

    const env = snakeCase(name).toUpperCase();
    if (processEnv[env]) {
      if (multiple) {
        // Technically not water-tight; assumes values for the string[] flag don't contain commas.
        envArgv.push(`--${name}`, ...processEnv[env].split(','));
      } else {
        envArgv.push(`--${name}`, processEnv[env]);
      }
    }

    const spec = [...desc];
    if (defaultValue !== undefined) {
      spec.push(`default: ${defaultValue}`);
    }
    spec.push(`env: ${env}`);

    const opt = {
      name,
      type: valueParser(name, elemType, terminalType),
      multiple,
      group,
      description: spec.join('\n') + '\n',
      typeLabel: literals.length
        ? literals.join(',')
        : multiple
        ? `${terminalType}[]`
        : terminalType,
    };
    optsWithoutDefaults.push(opt);
    optsWithDefaults.push({...opt, defaultValue});
  }

  const optsWithDefaults: DescribedOptionDefinition[] = [];
  const optsWithoutDefaults: DescribedOptionDefinition[] = [];
  const envArgv: string[] = [];

  try {
    for (const [name, val] of Object.entries(options)) {
      const {type} = val as {type: unknown};
      if (v.instanceOfAbstractType(val) || v.instanceOfAbstractType(type)) {
        addOption(name, val as Option<Value>);
      } else {
        const group = name;
        for (const [name, option] of Object.entries(val)) {
          addOption(name, option as Option<Value>, group);
        }
      }
    }

    const parsedArgs = merge(
      parseArgs(optsWithDefaults, argv),
      parseArgs(optsWithoutDefaults, envArgv),
      parseArgs(optsWithoutDefaults, argv),
    );

    const schema = configSchema(options);
    return v.parse(parsedArgs, schema);
  } catch (e) {
    logger.error?.(String(e));
    showUsage(optsWithDefaults, logger);
    throw e;
  }
}

function valueParser(
  flagName: string,
  elemType: v.Optional<unknown> | v.Type<unknown>,
  typeName: string | undefined,
) {
  if (!typeName || !PRIMITIVES.has(typeName)) {
    throw new TypeError(`--${flagName} flag has unsupported type ${typeName}`);
  }
  return (input: string) => {
    let value;
    switch (typeName) {
      case 'string':
        value = input;
        break;
      case 'boolean': {
        const bool = input.toLowerCase();
        if (['true', '1'].includes(bool)) {
          value = true;
        } else if (['false', '0'].includes(bool)) {
          value = false;
        } else {
          throw new TypeError(`Invalid input for --${flagName}: "${input}"`);
        }
        break;
      }
      default:
        try {
          value = JSON.parse(input);
        } catch (e) {
          throw new TypeError(`Invalid input for --${flagName}: "${input}"`, {
            cause: e,
          });
        }
        break;
    }
    const result = v.testOptional(value, elemType);
    if (result.ok) {
      return result.value;
    }
    throw new TypeError(result.error);
  };
}

function parseArgs(optionDefs: DescribedOptionDefinition[], argv: string[]) {
  function normalizeFlagValue(value: unknown) {
    // A --flag without value is parsed by commandLineArgs() to `null`,
    // but this is a common convention to set a boolean flag to true.
    return value === null ? true : value;
  }

  const {
    _all,
    _none: ungrouped,
    _unknown: unknown,
    ...groups
  } = commandLineArgs(optionDefs, {
    argv,
    partial: true,
  });
  if (unknown?.includes('--help') || unknown?.includes('-h')) {
    showUsage(optionDefs);
    process.exit(0);
  }

  // Strip the "group" prefix the flag name.
  for (const [groupName, flags] of Object.entries(groups ?? {})) {
    const prefix = groupName.length;
    const entries = Object.entries(flags);
    for (const [prefixedName, value] of entries) {
      const name = camelCase(prefixedName.slice(prefix));
      flags[name] = normalizeFlagValue(value);
      delete flags[prefixedName];
    }
  }

  // Normalize and promote ungrouped flags.
  for (const [name, value] of Object.entries(ungrouped ?? {})) {
    groups[name] = normalizeFlagValue(value);
  }
  return groups;
}

function showUsage(
  optionList: DescribedOptionDefinition[],
  logger: OptionalLogger = console,
) {
  logger.error?.(
    commandLineUsage({
      optionList,
      tableOptions: {
        columns: [
          {name: 'option', width: 35},
          {name: 'description', width: 70},
        ],
      },
    }),
  );
}

type DescribedOptionDefinition = OptionDefinition & {
  // Additional fields recognized by command-line-usage
  description?: string;
  typeLabel?: string | undefined;
};

const PRIMITIVES = new Set(['string', 'number', 'boolean']);