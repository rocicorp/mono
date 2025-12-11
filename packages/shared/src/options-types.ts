import type * as v from './valita.ts';

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

  /** Logged as a warning when parsed. */
  deprecated?: string[];

  /** One-character alias for getopt-style short flags, e.g. -m */
  alias?: string;

  /**
   * Exclude this flag from --help text. Used for internal flags.
   * Deprecated options are hidden by default.
   */
  hidden?: boolean;
};

export type Option = OptionType | WrappedOptionType;

// Related Options can be grouped.
export type Group = Record<string, Option>;

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
 * | Option          | Flag          | Env         |
 * | --------------  | ------------- | ----------- |
 * | port            | --port        | PORT        |
 * | numWorkers      | --num-workers | NUM_WORKERS |
 * | log: { level }  | --log-level   | LOG_LEVEL   |
 * | log: { format } | --log-format  | LOG_FORMAT  |
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
 */
export type Options = Record<string, Group | Option>;

/** Unwrap the Value type from an Option<V>. */
type ValueOf<T extends Option> =
  T extends v.Optional<infer V>
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
