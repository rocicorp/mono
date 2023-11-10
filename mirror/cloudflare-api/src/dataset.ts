import * as v from 'shared/src/valita.js';
import {
  Expressions,
  SelectBuilder,
  SelectClause,
  SelectSchema,
  Selectable,
  Where,
} from './sql.js';

// Workers Analytics Engine Datasets support string inputs ("blob1", "blob2", ... "blob20")
// and number inputs ("double1", "double2", ... "double20")
type InputSchema = Record<string, v.Type<string> | v.Type<number>>;
// A 'timestamp' column is internally populated when data points are written.
type OutputSchema<T extends InputSchema> = T & {timestamp: v.Type<Date>};

export const timestampSchema = v.string().chain(str => {
  try {
    // Strings from WAE look like: "2023-11-01 05:10:41".
    // Let the parser know that the Dates are in UTC by appending Z.
    str = str.endsWith('Z') ? str : `${str}Z`;
    return v.ok(new Date(str));
  } catch (e) {
    return v.err(e instanceof Error ? e : String(e));
  }
});

function outputSchema<T extends InputSchema>(
  inputSchema: v.ObjectType<T>,
): v.ObjectType<OutputSchema<T>> {
  return v.object({...inputSchema.shape, timestamp: timestampSchema});
}

type InputRow<T extends InputSchema> = v.Infer<v.ObjectType<T>>;
type InputCol<T extends InputSchema> = keyof InputRow<T>;

export class Dataset<T extends InputSchema> implements Selectable {
  readonly output: v.ObjectType<OutputSchema<T>>;
  readonly #name: string;

  readonly #blobs: InputCol<T>[] = [];
  readonly #doubles: InputCol<T>[] = [];
  readonly #columnAliases: Expressions<OutputSchema<T>> = {} as Expressions<
    OutputSchema<T>
  >;

  /**
   * Creates a Dataset for the specified dataset `name` and `schema`.
   *
   * Note that the order of the fields in the `schema` is important, as it
   * determines the mapping from the underlying dataset's `blob1`, `blob2` ... `blob20`
   * columns and `double1`, `double2`, ...`double20` columns to field name in the schema.
   * Namely, strings should defined in the `blob#` order, and doubles should
   * be defined in `double#` order.
   */
  constructor(name: string, input: v.ObjectType<T>) {
    this.output = outputSchema(input);
    this.#name = name;

    let blobNumber = 1;
    let doubleNumber = 1;
    Object.entries(input.shape).forEach(([name, kind]) => {
      const input = name as InputCol<T>;
      const output = name as keyof OutputSchema<T>;
      if (name === 'timestamp') {
        throw new Error('timestamp is a reserved output column name');
      }
      if (kind.name === 'string') {
        this.#blobs.push(input);
        this.#columnAliases[output] = `blob${blobNumber++}`;
      } else if (kind.name === 'number') {
        this.#doubles.push(input);
        this.#columnAliases[output] = `double${doubleNumber++}`;
      } else {
        // Should not be possible given that InputSchema only maps to strings and numbers.
        throw new Error(`Invalid column type for ${name}: ${kind.name}`);
      }
    });
    this.#columnAliases.timestamp = ''; // No alias necessary.
  }

  /** Creates a `WorkersAnalyticsEngineDataPoint` from an instance of the Input. */
  dataPoint(row: InputRow<T>) {
    return {
      blobs: this.#blobs.map(col => row[col] as string),
      doubles: this.#doubles.map(col => row[col] as number),
    };
  }

  /**
   * Starts a "SELECT *" statement that configures all column aliases according
   * to the InputSchema, e.g. `SELECT blob1 as teamID, blob2 as appID, ..., timestamp`
   */
  selectStar(): Where<OutputSchema<T>> {
    return this.select({schema: this.output, expr: this.#columnAliases});
  }

  /**
   * Selects all of the column aliases with additional expressions based on the aliases.
   */
  selectStarPlus<S extends SelectSchema>(
    more: SelectClause<S>,
  ): Where<OutputSchema<T> & S> {
    return this.select({
      schema: v.object({
        ...this.output.shape,
        ...more.schema.shape,
      }),
      expr: {
        ...this.#columnAliases,
        ...more.expr,
      },
    });
  }

  /**
   * Starts a custom "SELECT" statement with the given SelectSchema and accompanying
   * expressions for each alias.
   */
  select<S extends SelectSchema>(clause: SelectClause<S>): Where<S> {
    return SelectBuilder.create(this.#name, clause);
  }
}
