import type {OptionalLogger} from '@rocicorp/logger';
import {template} from 'chalk-template';
import type {OptionDefinition} from 'command-line-args';
import commandLineArgs from 'command-line-args';
import commandLineUsage, {type Section} from 'command-line-usage';
import {createDefu} from 'defu';
import {toKebabCase, toSnakeCase} from 'kasi';
import {stripVTControlCharacters as stripAnsi} from 'node:util';
import {assert} from './asserts.ts';
import {must} from './must.ts';
import type {
  Config,
  Group,
  Option,
  Options,
  WrappedOptionType,
} from './options-types.ts';
import * as v from './valita.ts';

export type {Config, Group, Option, Options, WrappedOptionType};

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

/**
 * Creates a defu instance that overrides arrays instead of merging them.
 */
const defu = createDefu((obj, key, value) => {
  if (!Array.isArray(value)) return;

  obj[key] = value;
  return true;
});

/**
 * Converts an Options instance into its corresponding {@link Config} schema.
 */
function configSchema<T extends Options>(
  options: T,
  envNamePrefix: string,
): v.Type<Config<T>> {
  function makeObjectType(options: Options | Group, group?: string) {
    return v.object(
      Object.fromEntries(
        Object.entries(options).map(
          ([name, value]): [string, OptionType | v.Type] => {
            const addErrorMessage = (t: OptionType) => {
              const {required} = getRequiredOrDefault(t);
              if (required) {
                // Adds an error message for required options that includes the
                // actual name of the option.
                const optionName = toSnakeCase(
                  `${envNamePrefix}${group ? group + '_' : ''}${name}`,
                ).toUpperCase();
                return (t as v.Type<string>)
                  .optional()
                  .assert(
                    val => val !== undefined,
                    `Missing required option ${optionName}`,
                  );
              }
              return t;
            };
            // OptionType
            if (v.instanceOfAbstractType(value)) {
              return [name, addErrorMessage(value)];
            }
            // WrappedOptionType
            const {type} = value;
            if (v.instanceOfAbstractType(type)) {
              return [name, addErrorMessage(type)];
            }
            // OptionGroup
            return [name, makeObjectType(value as Group, name)];
          },
        ),
      ),
    );
  }
  return makeObjectType(options) as v.Type<Config<T>>;
}

/**
 * Converts an Options instance into an "env schema", which is an object with
 * ENV names as its keys, mapped to optional or required string values
 * (corresponding to the optionality of the corresponding options).
 *
 * This is used as a format for encoding options for a multi-tenant version
 * of an app, with an envSchema for each tenant.
 */
export function envSchema<T extends Options>(options: T, envNamePrefix = '') {
  const fields: [string, v.Type<string> | v.Optional<string>][] = [];

  function addField(name: string, type: OptionType, group?: string) {
    const flag = group ? `${group}_${name}` : name;
    const env = toSnakeCase(`${envNamePrefix}${flag}`).toUpperCase();

    const {required} = getRequiredOrDefault(type);
    fields.push([env, required ? v.string() : v.string().optional()]);
  }

  function addFields(o: Options | Group, group?: string) {
    Object.entries(o).forEach(([name, value]) => {
      // OptionType
      if (v.instanceOfAbstractType(value)) {
        addField(name, value, group);
        return;
      }
      // WrappedOptionType
      const {type} = value;
      if (v.instanceOfAbstractType(type)) {
        addField(name, type, group);
        return;
      }
      // OptionGroup
      addFields(value as Group, name);
    });
  }

  addFields(options);

  return v.object(Object.fromEntries(fields));
}

// type TerminalType is not exported from badrap/valita
type TerminalType = Parameters<
  Parameters<v.Type<unknown>['toTerminals']>[0]
>[0];

function getRequiredOrDefault(type: OptionType) {
  const defaultResult = v.testOptional<Value>(undefined, type);
  return {
    required: !defaultResult.ok,
    defaultValue: defaultResult.ok ? defaultResult.value : undefined,
  };
}

export type ParseOptions = {
  /** Defaults to process.argv.slice(2) */
  argv?: string[];

  envNamePrefix?: string;

  description?: {header: string; content: string}[];

  /** Defaults to `false` */
  allowUnknown?: boolean;

  /** Defaults to `false` */
  allowPartial?: boolean;

  /** Defaults to `process.env`. */
  env?: NodeJS.ProcessEnv;

  /** Defaults to `true`. */
  emitDeprecationWarnings?: boolean;

  /** Defaults to `true`. When false, excludes default values from both config and env return values. */
  includeDefaults?: boolean;

  /** Defaults to `console` */
  logger?: OptionalLogger;

  /** Defaults to `process.exit` */
  exit?: (code?: number | string | null) => never;
};

export function parseOptions<T extends Options>(
  appOptions: T,
  opts: ParseOptions = {},
): Config<T> {
  return parseOptionsAdvanced(appOptions, opts).config;
}

export function parseOptionsAdvanced<T extends Options>(
  appOptions: T,
  opts: ParseOptions = {},
): {config: Config<T>; env: Record<string, string>; unknown?: string[]} {
  const {
    argv = process.argv.slice(2),
    envNamePrefix = '',
    description = [],
    allowUnknown = false,
    allowPartial = false,
    env: processEnv = process.env,
    emitDeprecationWarnings = true,
    includeDefaults = true,
    logger = console,
    exit = process.exit,
  } = opts;
  // The main logic for converting a valita Type spec to an Option (i.e. flag) spec.
  function addOption(field: string, option: WrappedOptionType, group?: string) {
    const {type, desc = [], deprecated, alias, hidden} = option;

    // The group name is prepended to the flag name.
    const flag = group ? toKebabCase(`${group}-${field}`) : toKebabCase(field);

    const {required, defaultValue} = getRequiredOrDefault(type);
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
    const env = toSnakeCase(`${envNamePrefix}${flag}`).toUpperCase();
    if (terminalTypes.size > 1) {
      throw new TypeError(`${env} has mixed types ${[...terminalTypes]}`);
    }
    assert(terminalTypes.size === 1);
    const terminalType = [...terminalTypes][0];

    if (processEnv[env]) {
      if (multiple) {
        // Technically not water-tight; assumes values for the string[] flag don't contain commas.
        envArgv.push(`--${flag}`, ...processEnv[env].split(','));
      } else {
        envArgv.push(`--${flag}`, processEnv[env]);
      }
    }
    names.set(flag, {field, env});

    const spec = [
      (required
        ? '{italic required}'
        : defaultValue !== undefined
          ? `default: ${JSON.stringify(defaultValue)}`
          : 'optional') + '\n',
    ];
    if (desc) {
      spec.push(...desc);
    }

    const typeLabel = [
      literals.size
        ? String([...literals].map(l => `{underline ${l}}`))
        : multiple
          ? `{underline ${terminalType}[]}`
          : `{underline ${terminalType}}`,
      `  ${env} env`,
    ];

    const opt = {
      name: flag,
      alias,
      type: valueParser(
        env,
        terminalType,
        logger,
        emitDeprecationWarnings ? deprecated : undefined,
      ),
      multiple,
      group,
      description: spec.join('\n') + '\n',
      typeLabel: typeLabel.join('\n') + '\n',
      hidden: hidden === undefined ? deprecated !== undefined : hidden,
    };
    optsWithoutDefaults.push(opt);
    optsWithDefaults.push({...opt, defaultValue});
  }

  const names = new Map<string, {field: string; env: string}>();
  const optsWithDefaults: DescribedOptionDefinition[] = [];
  const optsWithoutDefaults: DescribedOptionDefinition[] = [];
  const envArgv: string[] = [];

  try {
    for (const [name, val] of Object.entries(appOptions)) {
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

    const [defaults, env1, unknown] = parseArgs(optsWithDefaults, argv, names);
    const [fromEnv, env2] = parseArgs(optsWithoutDefaults, envArgv, names);
    const [withoutDefaults, env3] = parseArgs(optsWithoutDefaults, argv, names);

    switch (unknown?.[0]) {
      case undefined:
        break;
      case '--help':
      case '-h':
        showUsage(optsWithDefaults, description, logger);
        exit(0);
        break;
      default:
        if (!allowUnknown) {
          logger.error?.('Invalid arguments:', unknown);
          showUsage(optsWithDefaults, description, logger);
          exit(0);
        }
        break;
    }

    const parsedArgs = includeDefaults
      ? defu(withoutDefaults, fromEnv, defaults)
      : defu(withoutDefaults, fromEnv);
    const env = includeDefaults
      ? {...env1, ...env2, ...env3}
      : {...env2, ...env3};

    let schema = configSchema(appOptions, envNamePrefix);
    if (allowPartial || !includeDefaults) {
      // TODO: Type configSchema() to return a v.ObjectType<...>
      schema = v.deepPartial(schema as v.ObjectType) as v.Type<Config<T>>;
    }
    return {
      config: v.parse(parsedArgs, schema),
      env,
      ...(unknown ? {unknown} : {}),
    };
  } catch (e) {
    logger.error?.(String(e));
    showUsage(optsWithDefaults, description, logger);
    throw e;
  }
}

function valueParser(
  optionName: string,
  typeName: string,
  logger: OptionalLogger,
  deprecated: string[] | undefined,
) {
  return (input: string) => {
    if (deprecated) {
      logger.warn?.(
        template(
          `\n${optionName} is deprecated:\n` + deprecated.join('\n') + '\n',
        ),
      );
    }
    switch (typeName) {
      case 'string':
        return input;
      case 'boolean':
        return parseBoolean(optionName, input);
      case 'number': {
        const val = Number(input);
        if (Number.isNaN(val)) {
          throw new TypeError(`Invalid input for ${optionName}: "${input}"`);
        }
        return val;
      }
      default:
        // Should be impossible given the constraints of `Option`
        throw new TypeError(
          `${optionName} option has unsupported type ${typeName}`,
        );
    }
  };
}

function parseArgs(
  optionDefs: DescribedOptionDefinition[],
  argv: string[],
  names: Map<string, {field: string; env: string}>,
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
    ...config
  } = commandLineArgs(optionDefs, {
    argv,
    partial: true,
  });

  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const result: Record<string, any> = {};
  const envObj: Record<string, string> = {};

  function addFlag(flagName: string, value: unknown, group?: string) {
    const {field, env} = must(names.get(flagName));
    const normalized = normalizeFlagValue(value);
    if (group) {
      result[group][field] = normalized;
    } else {
      result[field] = normalized;
    }
    envObj[env] = String(normalized);
  }

  for (const [flagName, value] of Object.entries(ungrouped ?? {})) {
    addFlag(flagName, value);
  }

  // Then handle (potentially) grouped flags
  for (const [name, value] of Object.entries(config)) {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
      addFlag(name, value); // Flag, not a group
    } else {
      const group = name;
      result[group] = {};
      for (const [flagName, flagValue] of Object.entries(value)) {
        addFlag(flagName, flagValue, group);
      }
    }
  }

  return [result, envObj, unknown] as const;
}

export function parseBoolean(optionName: string, input: string) {
  const bool = input.toLowerCase();
  if (['true', '1'].includes(bool)) {
    return true;
  } else if (['false', '0'].includes(bool)) {
    return false;
  }
  throw new TypeError(`Invalid input for ${optionName}: "${input}"`);
}

function showUsage(
  optionList: DescribedOptionDefinition[],
  description: {header: string; content: string}[] = [],
  logger: OptionalLogger = console,
) {
  const hide: string[] = [];
  let leftWidth = 35;
  let rightWidth = 70;
  optionList.forEach(({name, typeLabel, description, hidden}) => {
    if (hidden) {
      hide.push(name);
    }
    const text = template(`${name} ${typeLabel ?? ''}`);
    const lines = stripAnsi(text).split('\n');
    for (const l of lines) {
      leftWidth = Math.max(leftWidth, l.length + 2);
    }
    const desc = stripAnsi(template(description ?? '')).split('\n');
    for (const l of desc) {
      rightWidth = Math.max(rightWidth, l.length + 2);
    }
  });

  const sections: Section[] = [
    {
      optionList,
      reverseNameOrder: true, // Display --flag-name before -alias
      hide,
      tableOptions: {
        columns: [
          {name: 'option', width: leftWidth},
          {name: 'description', width: rightWidth},
        ],
        noTrim: true,
      },
    },
  ];

  if (description) {
    sections.unshift(...description);
  }

  logger.info?.(commandLineUsage(sections));
}

type DescribedOptionDefinition = OptionDefinition & {
  // Additional fields recognized by command-line-usage
  description?: string;
  typeLabel?: string | undefined;
  hidden?: boolean | undefined;
};
