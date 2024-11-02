import type {OptionalLogger} from '@rocicorp/logger';
import camelcase from 'camelcase';
import type {OptionDefinition} from 'command-line-args';
import commandLineArgs from 'command-line-args';
import commandLineUsage from 'command-line-usage';
import merge from 'lodash.merge';
import snakeCase from 'lodash.snakecase';
import {assert} from '../../../shared/src/asserts.js';
import * as v from '../../../shared/src/valita.js';

type Primitive = number | string | boolean;
type Value = Primitive | Array<Primitive>;
type OptionType<T extends Value> = v.Type<T> | v.Optional<T>;
export type WrappedOptionType<T extends Value> = {
  type: OptionType<T>;

  /** Description lines to be displayed in --help. */
  desc?: string[];

  /** One-character alias for getopt-style short flags, e.g. -m */
  alias?: string;

  /**
   * Capitalize all letters in the name when part of a grouped flag.
   * This is suitable for acronyms like "db", "id", "url", etc.
   *
   * e.g. `shard: { id: { allCaps: true } } ==> --shardID`
   */
  allCaps?: boolean;
};

export type Option<T extends Value> = OptionType<T> | WrappedOptionType<T>;

// Related Options can be grouped.
type Group = Record<string, Option<Value>>;

/**
 * # Options
 *
 * An `Options` object specifies of a set of (possibly grouped) configuration
 * values that are parsed from environment variables and/or command line flags.
 *
 * Each option is represented by a `valita` schema object. The `Options`
 * type supports one level of grouping for organizing related options.
 *
 * ```ts
 * {
 *   port: v.number().default(8080),
 *
 *   numWorkers: v.number(),
 *
 *   log: {
 *     level: v.union(v.literal('debug'), v.literal('info'), ...),
 *     format: v.union(v.literal('text'), v.literal('json')).default('text'),
 *   }
 * }
 * ```
 *
 * {@link parseOptions()} will use an `Options` object to populate a {@link Config}
 * instance of the corresponding shape, consulting SNAKE_CASE environment variables
 * and/or camelCase command line flags, with flags taking precedence, based on the field
 * (and group) names:
 *
 * | Option          | Flag         | Env         |
 * | --------------  | ------------ | ----------- |
 * | port            | --port       | PORT        |
 * | numWorkers      | --numWorkers | NUM_WORKERS |
 * | log: { level }  | --logLevel   | LOG_LEVEL   |
 * | log: { format } | --logFormat  | LOG_FORMAT  |
 *
 * `Options` supports:
 * * primitive valita types `string`, `number`, `boolean`
 * * single-type arrays or tuples of primitives
 * * optional values
 * * default values
 *
 * ### Additional Flag Configuration
 *
 * {@link parseOptions()} will generate a usage guide that is displayed for
 * the `--help` or `-h` flags, displaying the flag name, env name, value
 * type (or enumeration), and default values based on the valita schema.
 *
 * For additional configuration, each object can instead by represented by
 * a {@link WrappedOptionType}, where the valita schema is held in the `type`
 * field, along with additional optional fields:
 * * `desc` for documentation displayed in `--help`
 * * `alias` for getopt-style short flags like `-m`
 * * `allCaps` for acronym fields that should be in all caps when appended to
 *   a group name to produce a camelcase flag name.
 */
export type Options = Record<string, Group | Option<Value>>;

/** Unwrap the Value type from an Option<V>. */
type ValueOf<T extends Option<Value>> = T extends v.Optional<infer V>
  ? V | undefined
  : T extends v.Type<infer V>
  ? V
  : T extends WrappedOptionType<infer V>
  ? V
  : never;

/**
 * A Config is an object containing values parsed from an {@link Options} object.
 *
 * Example:
 *
 * ```ts
 * {
 *   port: number;
 *
 *   numWorkers: number;
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
            // OptionType
            if (v.instanceOfAbstractType(value)) {
              return [name, value];
            }
            // WrappedOptionType
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
  function addOption(
    name: string,
    option: WrappedOptionType<Value>,
    group?: string,
  ) {
    const {type, desc = [], alias, allCaps} = option;

    // The group name is prepended to the flag name and stripped in parseArgs().
    if (group) {
      name = group
        ? camelcase(`${group}_${allCaps ? name.toUpperCase() : name}`, {
            preserveConsecutiveUppercase: true,
          })
        : name;
    }

    const defaultResult = v.testOptional(undefined, type);
    const defaultValue = defaultResult.ok ? defaultResult.value : undefined;

    const literals: string[] = [];
    let {multiple, elemType} = getElemType(type, name);
    let terminalType: string | undefined;

    type.toTerminals(t => {
      let typeName: string;
      switch (t.name) {
        case 'undefined':
        case 'optional':
          return;
        case 'array': {
          multiple = true;
          const {elemType: newElemType} = getElemType(
            t as OptionType<Value>,
            name,
          );
          elemType = newElemType;
          typeName = elemType.name;
          break;
        }
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
      spec.push(`default: ${JSON.stringify(defaultValue)}`);
    }
    spec.push(`env: ${env}`);

    const opt = {
      name,
      alias,
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
      if (v.instanceOfAbstractType(val)) {
        addOption(name, {type: val});
      } else if (v.instanceOfAbstractType(type)) {
        addOption(name, val as WrappedOptionType<Value>);
      } else {
        const group = name;
        for (const [name, option] of Object.entries(val as Group)) {
          const wrapped = v.instanceOfAbstractType(option)
            ? {type: option}
            : option;
          addOption(name, wrapped, group);
        }
      }
    }

    const parsedArgs = merge(
      parseArgs(optsWithDefaults, argv, logger),
      parseArgs(optsWithoutDefaults, envArgv, logger),
      parseArgs(optsWithoutDefaults, argv, logger),
    );

    const schema = configSchema(options);
    return v.parse(parsedArgs, schema);
  } catch (e) {
    if (!(e instanceof ExitAfterUsage)) {
      logger.error?.(String(e));
      showUsage(optsWithDefaults, logger);
    }
    throw e;
  }
}

function getElemType(
  type: OptionType<Value>,
  flagName: string,
): {
  multiple: boolean;
  elemType: OptionType<Value>;
} {
  const multiple = type.name === 'array';
  if (!multiple) {
    return {multiple, elemType: type};
  }

  const a = type as v.ArrayType<v.Type<Value>>;
  const types = [
    ...a.prefix,
    ...(a.rest ? [a.rest] : []),
    ...a.suffix,
  ] as v.Type<Value>[];
  assert(types.length);

  const typeNames = new Set(types.map(t => t.name));
  if (typeNames.size > 1) {
    throw new TypeError(`--${flagName} has mixed types ${[...typeNames]}`);
  }
  return {multiple, elemType: types[0]};
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

function parseArgs(
  optionDefs: DescribedOptionDefinition[],
  argv: string[],
  logger: OptionalLogger,
) {
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
    showUsage(optionDefs, logger);
    throw new ExitAfterUsage();
  }

  // Strip the "group" prefix the flag name.
  for (const [groupName, flags] of Object.entries(groups ?? {})) {
    const prefix = groupName.length;
    const entries = Object.entries(flags);
    for (const [prefixedName, value] of entries) {
      const name = camelcase(prefixedName.slice(prefix));
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

export class ExitAfterUsage extends Error {}
