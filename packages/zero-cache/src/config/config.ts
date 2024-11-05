import type {OptionalLogger} from '@rocicorp/logger';
import {Ansis, italic, underline} from 'ansis';
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

type RequiredOptionType =
  | v.Type<string>
  | v.Type<number>
  | v.Type<boolean>
  | v.Type<string[]>
  | v.Type<number[]>
  | v.Type<boolean[]>;

type OptionalOptionType =
  | v.Optional<string>
  | v.Optional<number>
  | v.Optional<boolean>
  | v.Optional<string[]>
  | v.Optional<number[]>
  | v.Optional<boolean[]>;

type OptionType = RequiredOptionType | OptionalOptionType;

export type WrappedOptionType = {
  type: OptionType;

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

export type Option = OptionType | WrappedOptionType;

// Related Options can be grouped.
type Group = Record<string, Option>;

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
export type Options = Record<string, Group | Option>;

/** Unwrap the Value type from an Option<V>. */
type ValueOf<T extends Option> = T extends v.Optional<infer V>
  ? V | undefined
  : T extends v.Type<infer V>
  ? V
  : T extends WrappedOptionType
  ? ValueOf<T['type']>
  : never;

type Required =
  | RequiredOptionType
  | (WrappedOptionType & {type: RequiredOptionType});
type Optional =
  | OptionalOptionType
  | (WrappedOptionType & {type: OptionalOptionType});

// Type the fields for optional options as `field?`
type ConfigGroup<G extends Group> = {
  [P in keyof G as G[P] extends Required ? P : never]: ValueOf<G[P]>;
} & {
  // Values for optional options are in optional fields.
  [P in keyof G as G[P] extends Optional ? P : never]?: ValueOf<G[P]>;
};

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
  [P in keyof O as O[P] extends Required | Group
    ? P
    : never]: O[P] extends Required
    ? ValueOf<O[P]>
    : O[P] extends Group
    ? ConfigGroup<O[P]>
    : never;
} & {
  // Values for optional options are in optional fields.
  [P in keyof O as O[P] extends Optional ? P : never]?: O[P] extends Optional
    ? ValueOf<O[P]>
    : never;
};

/**
 * Converts an Options instance into its corresponding {@link Config} schema.
 */
function configSchema<T extends Options>(options: T): v.Type<Config<T>> {
  function makeObjectType(options: Options | Group) {
    return v.object(
      Object.fromEntries(
        Object.entries(options).map(
          ([name, value]): [string, OptionType | v.Type] => {
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

// type TerminalType is not exported from badrap/valita
type TerminalType = Parameters<
  Parameters<v.Type<unknown>['toTerminals']>[0]
>[0];

export function parseOptions<T extends Options>(
  options: T,
  argv: string[],
  processEnv = process.env,
  logger: OptionalLogger = console,
): Config<T> {
  // The main logic for converting a valita Type spec to an Option (i.e. flag) spec.
  function addOption(name: string, option: WrappedOptionType, group?: string) {
    const {type, desc = [], alias, allCaps} = option;

    // The group name is prepended to the flag name and stripped in parseArgs().
    if (group) {
      name = group
        ? camelcase(`${group}_${allCaps ? name.toUpperCase() : name}`, {
            preserveConsecutiveUppercase: true,
          })
        : name;
    }

    const defaultResult = v.testOptional<Value>(undefined, type);
    const required = !defaultResult.ok;
    const defaultValue = defaultResult.ok ? defaultResult.value : undefined;

    let multiple = type.name === 'array';
    const literals = new Set<string>();
    const terminalTypes = new Set<string>();

    type.toTerminals(getTerminalTypes);

    function getTerminalTypes(t: TerminalType) {
      switch (t.name) {
        case 'undefined':
        case 'optional':
          break;
        case 'array': {
          multiple = true;
          t.prefix.forEach(t => t.toTerminals(getTerminalTypes));
          t.rest?.toTerminals(getTerminalTypes);
          t.suffix.forEach(t => t.toTerminals(getTerminalTypes));
          break;
        }
        case 'literal':
          literals.add(String(t.value));
          terminalTypes.add(typeof t.value);
          break;
        default:
          terminalTypes.add(t.name);
          break;
      }
    }
    if (terminalTypes.size > 1) {
      throw new TypeError(`--${name} has mixed types ${[...terminalTypes]}`);
    }
    assert(terminalTypes.size === 1);
    const terminalType = [...terminalTypes][0];

    const env = snakeCase(name).toUpperCase();
    if (processEnv[env]) {
      if (multiple) {
        // Technically not water-tight; assumes values for the string[] flag don't contain commas.
        envArgv.push(`--${name}`, ...processEnv[env].split(','));
      } else {
        envArgv.push(`--${name}`, processEnv[env]);
      }
    }

    const spec = [
      (required
        ? italic('required')
        : defaultValue !== undefined
        ? `default: ${JSON.stringify(defaultValue)}`
        : 'optional') + '\n',
    ];
    if (desc) {
      spec.push(...desc);
    }

    const typeLabel = [
      literals.size
        ? String([...literals].map(underline))
        : multiple
        ? underline(`${terminalType}[]`)
        : underline(terminalType),
      `  ${env} env`,
    ];

    const opt = {
      name,
      alias,
      type: valueParser(name, terminalType),
      multiple,
      group,
      description: spec.join('\n') + '\n',
      typeLabel: typeLabel.join('\n') + '\n',
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
        addOption(name, val as WrappedOptionType);
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

function valueParser(flagName: string, typeName: string) {
  return (input: string) => {
    switch (typeName) {
      case 'string':
        return input;
      case 'boolean': {
        const bool = input.toLowerCase();
        if (['true', '1'].includes(bool)) {
          return true;
        } else if (['false', '0'].includes(bool)) {
          return false;
        }
        throw new TypeError(`Invalid input for --${flagName}: "${input}"`);
      }
      case 'number': {
        const val = Number(input);
        if (Number.isNaN(val)) {
          throw new TypeError(`Invalid input for --${flagName}: "${input}"`);
        }
        return val;
      }
      default:
        // Should be impossible given the constraints of `Option`
        throw new TypeError(
          `--${flagName} flag has unsupported type ${typeName}`,
        );
    }
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

const ansis = new Ansis();

function showUsage(
  optionList: DescribedOptionDefinition[],
  logger: OptionalLogger = console,
) {
  let leftWidth = 35;
  let rightWidth = 70;
  optionList.forEach(({name, typeLabel, description}) => {
    const lines = ansis.strip(`${name} ${typeLabel ?? ''}`).split('\n');
    for (const l of lines) {
      leftWidth = Math.max(leftWidth, l.length + 2);
    }
    const desc = ansis.strip(description ?? '').split('\n');
    for (const l of desc) {
      rightWidth = Math.max(rightWidth, l.length + 2);
    }
  });

  logger.error?.(
    commandLineUsage({
      optionList,
      reverseNameOrder: true, // Display --flagName before -alias
      tableOptions: {
        columns: [
          {name: 'option', width: leftWidth},
          {name: 'description', width: rightWidth},
        ],
        noTrim: true,
      },
    }),
  );
}

type DescribedOptionDefinition = OptionDefinition & {
  // Additional fields recognized by command-line-usage
  description?: string;
  typeLabel?: string | undefined;
};

export class ExitAfterUsage extends Error {}
