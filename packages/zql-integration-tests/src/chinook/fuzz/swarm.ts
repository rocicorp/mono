/**
 * **L2 — swarm** (ported from rusty-ivm `rindle-fuzz/src/swarm.rs`, design §2 L2). Per
 * run, draw a random **feature mask** that *disables* a subset of decoration axes, so
 * each run generates from a small feature alphabet. Empirically (Groce et al., "Swarm
 * Testing") this finds more bugs than feature-rich random: rich queries crowd out the
 * simple combinations, and some bugs surface only when a feature is *absent*.
 *
 * It is a thin layer over the L1 machinery: a disabled axis is pinned to its baseline
 * (index 0 = the `none` value), then the assignment is lowered with the existing
 * {@link decorate} / {@link decorateChild}.
 */

import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {AXES, N_AXES, tables} from './axes.ts';
import {childDecorationPairs, decorate, decorateChild} from './cover.ts';
import type {Rng} from './rng.ts';

/**
 * A feature mask: which decoration axes are **enabled** this run, and whether to nest the
 * query under a parent. A disabled axis is pinned to its baseline (index 0 = the `none`
 * value), shrinking the feature alphabet.
 */
export class Mask {
  readonly axes: readonly boolean[];
  readonly nest: boolean;

  constructor(axes: readonly boolean[], nest: boolean) {
    this.axes = axes;
    this.nest = nest;
  }

  /** Every feature enabled (the alphabet the L4 random tail draws from). */
  static full(): Mask {
    return new Mask(new Array(N_AXES).fill(true), true);
  }

  /** A random subset of features enabled. */
  static random(rng: Rng): Mask {
    return new Mask(
      Array.from({length: N_AXES}, () => rng.bool()),
      rng.bool(),
    );
  }

  on(axis: number): boolean {
    return this.axes[axis];
  }
}

/** A random axis assignment under `mask` (disabled axes forced to value 0). */
function randomAssignment(rng: Rng, mask: Mask): number[] {
  return AXES.map((ax, i) => (mask.on(i) ? rng.int(ax.values.length) : 0));
}

/**
 * Generate one random query under `mask`: a root-only decorated query, or — when the mask
 * enables nesting — a decorated nested collection. `true` iff EXISTS-bearing. `null` if
 * the random pick is unrealizable on the chosen table (the caller retries / skips).
 */
export function swarmGen(rng: Rng, mask: Mask): [AnyQuery, boolean] | null {
  const a = randomAssignment(rng, mask);
  if (mask.nest) {
    const pair = rng.choose(childDecorationPairs());
    return pair ? decorateChild(pair[0], pair[1], a) : null;
  }
  const root = rng.choose(tables());
  return root ? decorate(root, a) : null;
}
