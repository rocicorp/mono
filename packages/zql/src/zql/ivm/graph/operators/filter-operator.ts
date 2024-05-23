import type {SimpleOperator} from '../../../ast/ast.js';
import {genFilter} from '../../../util/iterables.js';
import type {Multiset} from '../../multiset.js';
import {getValueFromEntity} from '../../source/util.js';
import type {PipelineEntity} from '../../types.js';
import type {DifferenceStream, Listener} from '../difference-stream.js';
import type {Request} from '../message.js';
import {OperatorBase} from './operator.js';

export class FilterOperator<I extends PipelineEntity> extends OperatorBase<I> {
  readonly #listener: Listener<I>;
  readonly #input: DifferenceStream<I>;

  readonly #column: readonly [string | null, string];
  readonly #fn: (lhs: unknown) => boolean;
  readonly #op;
  readonly #value: unknown;

  constructor(
    input: DifferenceStream<I>,
    output: DifferenceStream<I>,
    selector: readonly [string | null, string],
    operator: SimpleOperator,
    value: unknown,
  ) {
    super(output);
    this.#listener = {
      newDifference: (version, data, reply) => {
        output.newDifference(version, this.#filter(data), reply);
      },
      commit: version => {
        this.commit(version);
      },
    };
    input.addDownstream(this.#listener);
    this.#input = input;
    this.#fn = getOperator(operator, value);
    this.#column = selector;
    this.#op = operator;
    this.#value = value;
  }

  #filter(data: Multiset<I>) {
    return genFilter(data, e =>
      this.#fn(getValueFromEntity(e[0], this.#column)),
    );
  }

  messageUpstream(message: Request): void {
    message = {
      ...message,
      hoistedConditions: message.hoistedConditions.concat({
        selector: this.#column,
        op: this.#op,
        value: this.#value,
      }),
    };
    this.#input.messageUpstream(message, this.#listener);
  }

  destroy() {
    this.#input.removeDownstream(this.#listener);
  }
}

// We're well-typed in the query builder so once we're down here
// we can assume that the operator is valid.
export function getOperator(
  op: SimpleOperator,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rhs: any,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): (lhs: any) => boolean {
  switch (op) {
    case '=':
      return lhs => lhs === rhs;
    case '!=':
      return lhs => lhs !== rhs;
    case '<':
      return lhs => lhs < rhs;
    case '>':
      return lhs => lhs > rhs;
    case '>=':
      return lhs => lhs >= rhs;
    case '<=':
      return lhs => lhs <= rhs;
    case 'IN':
      return lhs => rhs.includes(lhs);
    case 'NOT IN':
      return lhs => !rhs.includes(lhs);
    case 'LIKE':
      return getLikeOp(rhs, '');
    case 'NOT LIKE':
      return not(getLikeOp(rhs, ''));
    case 'ILIKE':
      return getLikeOp(rhs, 'i');
    case 'NOT ILIKE':
      return not(getLikeOp(rhs, 'i'));
    case 'INTERSECTS': {
      const rhSet = new Set(rhs);
      return lhs => {
        if (Array.isArray(lhs)) {
          return lhs.some(x => rhSet.has(x));
        }
        return rhSet.has(lhs);
      };
    }
    case 'DISJOINT': {
      const rhSet = new Set(rhs);
      return lhs => {
        if (Array.isArray(lhs)) {
          return lhs.every(x => !rhSet.has(x));
        }
        return !rhSet.has(lhs);
      };
    }
    case 'SUPERSET': {
      return lhs => {
        if (rhs.length === 0) {
          return true;
        }
        if (Array.isArray(lhs)) {
          const lhSet = new Set(lhs);
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          return rhs.every((x: any) => lhSet.has(x));
        }
        return rhs.length === 1 && lhs === rhs[0];
      };
    }
    case 'CONGRUENT': {
      const rhSet = new Set(rhs);
      return lhs => {
        if (Array.isArray(lhs)) {
          return rhSet.size === lhs.length && lhs.every(x => rhSet.has(x));
        }
        return rhs.length === 1 && lhs === rhs[0];
      };
    }
    case 'INCONGRUENT': {
      const rhSet = new Set(rhs);
      return lhs => {
        if (Array.isArray(lhs)) {
          return rhSet.size !== lhs.length || !lhs.every(x => rhSet.has(x));
        }
        return rhs.length !== 1 || lhs !== rhs[0];
      };
    }
    case 'SUBSET': {
      const rhSet = new Set(rhs);
      return lhs => {
        if (Array.isArray(lhs)) {
          return lhs.every(x => rhSet.has(x));
        }
        return rhSet.has(lhs);
      };
    }
  }
  throw new Error(`unexpected op: ${op}`);
}

function not<T>(f: (lhs: T) => boolean) {
  return (lhs: T) => !f(lhs);
}

function getLikeOp(pattern: string, flags: 'i' | ''): (lhs: string) => boolean {
  // if lhs does not contain '%' or '_' then it is a simple string comparison.
  // if it does contain '%' or '_' then it is a regex comparison.
  // '%' is a wildcard for any number of characters
  // '_' is a wildcard for a single character
  // Postgres SQL allows escaping using `\`.

  if (!/_|%|\\/.test(pattern)) {
    if (flags === 'i') {
      const rhsLower = pattern.toLowerCase();
      return (lhs: string) => lhs.toLowerCase() === rhsLower;
    }
    return (lhs: string) => lhs === pattern;
  }
  const re = patternToRegExp(pattern, flags);
  return (lhs: string) => re.test(lhs);
}

const specialCharsRe = /[$()*+.?[\]\\^{|}]/;

function patternToRegExp(source: string, flags: '' | 'i' = ''): RegExp {
  // There are a few cases:
  // % => .*
  // _ => .
  // \x => \x for any x except special regexp chars
  // special regexp chars => \special regexp chars
  let pattern = '^';
  for (let i = 0; i < source.length; i++) {
    let c = source[i];
    switch (c) {
      case '%':
        pattern += '.*';
        break;
      case '_':
        pattern += '.';
        break;

      // @ts-expect-error fallthrough
      case '\\':
        if (i === source.length - 1) {
          throw new Error('LIKE pattern must not end with escape character');
        }
        i++;
        c = source[i];

      // fall through
      default:
        if (specialCharsRe.test(c)) {
          pattern += '\\';
        }
        pattern += c;

        break;
    }
  }
  return new RegExp(pattern + '$', flags);
}
