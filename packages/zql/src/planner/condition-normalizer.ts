import type {
  AST,
  Condition,
  CorrelatedSubquery,
  CorrelatedSubqueryCondition,
  LiteralValue,
  SimpleCondition,
  ValuePosition,
} from '../../../zero-protocol/src/ast.ts';
import {planIdSymbol} from '../../../zero-protocol/src/ast.ts';

const TRUE: Condition = {type: 'and', conditions: []};
const FALSE: Condition = {type: 'or', conditions: []};

export function normalizePlannerAST(ast: AST): AST {
  return {
    ...ast,
    where: ast.where ? normalizeCondition(ast.where) : undefined,
    related: ast.related?.map(related => ({
      ...related,
      subquery: normalizePlannerAST(related.subquery),
    })),
  };
}

function normalizeCondition(condition: Condition): Condition {
  switch (condition.type) {
    case 'simple':
      return normalizeSimpleCondition(condition);
    case 'correlatedSubquery':
      return normalizeCorrelatedSubquery(condition);
    case 'and':
      return buildAnd(condition.conditions.map(normalizeCondition));
    case 'or':
      return buildOr(condition.conditions.map(normalizeCondition));
  }
}

function normalizeSimpleCondition(condition: SimpleCondition): Condition {
  if (condition.right.type !== 'literal') {
    return condition;
  }

  const value = condition.right.value;
  if (!Array.isArray(value)) {
    return condition;
  }

  if (condition.op === 'IN') {
    const values = dedupeInLiteralValues(value);
    switch (values.length) {
      case 0:
        return FALSE;
      case 1:
        return {
          ...condition,
          op: '=',
          right: {type: 'literal', value: values[0]},
        };
      default:
        return {
          ...condition,
          right: {type: 'literal', value: values},
        };
    }
  }

  if (condition.op === 'NOT IN') {
    const values = dedupeInLiteralValues(value);
    switch (values.length) {
      case 0:
        return condition;
      case 1:
        return {
          ...condition,
          op: '!=',
          right: {type: 'literal', value: values[0]},
        };
      default:
        return {
          ...condition,
          right: {type: 'literal', value: values},
        };
    }
  }

  return condition;
}

function normalizeCorrelatedSubquery(
  condition: CorrelatedSubqueryCondition,
): Condition {
  const {[planIdSymbol]: _planId, ...conditionWithoutPlanId} = condition;
  return collapseImpossibleCorrelatedSubquery({
    ...conditionWithoutPlanId,
    related: {
      ...condition.related,
      subquery: normalizePlannerAST(condition.related.subquery),
    },
  });
}

function buildAnd(conditions: readonly Condition[]): Condition {
  const flattened = flatten('and', conditions);
  if (flattened.some(isAlwaysFalse)) {
    return FALSE;
  }

  const deduped = dedupe(
    flattened.filter(condition => !isAlwaysTrue(condition)),
  );
  const intersected = intersectEquivalentAndPredicates(deduped);
  if (intersected.some(isAlwaysFalse)) {
    return FALSE;
  }

  switch (intersected.length) {
    case 0:
      return TRUE;
    case 1:
      return intersected[0];
    default:
      return {type: 'and', conditions: intersected};
  }
}

function buildOr(conditions: readonly Condition[]): Condition {
  const flattened = flatten('or', conditions);
  if (flattened.some(isAlwaysTrue)) {
    return TRUE;
  }

  const deduped = dedupe(
    flattened.filter(condition => !isAlwaysFalse(condition)),
  );
  if (deduped.length === 0) {
    return FALSE;
  }

  const compacted = absorbRedundantOrBranches(
    mergeEquivalentOrPredicates(deduped),
  );

  const factored = factorCommonConjuncts(compacted);
  if (factored) {
    return normalizeCondition(factored);
  }

  const merged = mergeSameRelationshipExists(compacted);
  if (merged.some(isAlwaysTrue)) {
    return TRUE;
  }

  const surviving = merged.filter(condition => !isAlwaysFalse(condition));
  switch (surviving.length) {
    case 0:
      return FALSE;
    case 1:
      return surviving[0];
    default:
      return {type: 'or', conditions: surviving};
  }
}

type InLiteralValue = string | number | boolean;

type ColumnDomainBase = {
  readonly left: ValuePosition;
};

// include undefined means the column can still take any literal value.
// exclude then removes known impossible values from that open domain.
type ColumnDomain = ColumnDomainBase & {
  readonly include: readonly InLiteralValue[] | undefined;
  readonly exclude: readonly InLiteralValue[];
};

type PositiveColumnDomain = ColumnDomainBase & {
  readonly include: readonly InLiteralValue[];
};

type DomainPredicate<TDomain extends ColumnDomainBase> = {
  readonly domain: TDomain;
  readonly changed: boolean;
};

type DomainGroup<TDomain extends ColumnDomainBase> = {
  readonly firstIndex: number;
  domain: TDomain;
  changed: boolean;
};

type DomainRewrite<TDomain extends ColumnDomainBase> = {
  readonly extract: (
    condition: Condition,
  ) => DomainPredicate<TDomain> | undefined;
  readonly combine: (left: TDomain, right: TDomain) => TDomain;
  readonly lower: (domain: TDomain) => Condition;
};

function intersectEquivalentAndPredicates(
  conditions: readonly Condition[],
): Condition[] {
  return rewriteColumnDomains(conditions, {
    extract: columnDomainFromCondition,
    combine: intersectColumnDomains,
    lower: columnDomainToCondition,
  });
}

function mergeEquivalentOrPredicates(
  conditions: readonly Condition[],
): Condition[] {
  return rewriteColumnDomains(conditions, {
    extract: positiveColumnDomainFromCondition,
    combine: unionPositiveColumnDomains,
    lower: positiveColumnDomainToCondition,
  });
}

function rewriteColumnDomains<TDomain extends ColumnDomainBase>(
  conditions: readonly Condition[],
  rewrite: DomainRewrite<TDomain>,
): Condition[] {
  const rewritten: Array<Condition | undefined> = [...conditions];
  const groups = new Map<string, DomainGroup<TDomain>>();
  for (const [index, condition] of conditions.entries()) {
    const predicate = rewrite.extract(condition);
    if (!predicate) {
      continue;
    }

    const key = domainKey(predicate.domain);
    const group = groups.get(key);
    if (!group) {
      groups.set(key, {
        firstIndex: index,
        domain: predicate.domain,
        changed: predicate.changed,
      });
      continue;
    }

    rewritten[index] = undefined;
    group.changed = true;
    group.domain = rewrite.combine(group.domain, predicate.domain);
  }

  for (const group of groups.values()) {
    if (!group.changed) {
      continue;
    }
    rewritten[group.firstIndex] = rewrite.lower(group.domain);
  }

  return rewritten.filter((condition): condition is Condition => !!condition);
}

function columnDomainFromCondition(
  condition: Condition,
): DomainPredicate<ColumnDomain> | undefined {
  if (condition.type !== 'simple' || condition.right.type !== 'literal') {
    return undefined;
  }

  const {value} = condition.right;
  switch (condition.op) {
    case '=':
      return isInLiteralValue(value)
        ? finiteColumnDomain(condition.left, [value])
        : undefined;
    case 'IN':
      return Array.isArray(value) && value.every(isInLiteralValue)
        ? finiteColumnDomain(condition.left, value)
        : undefined;
    case '!=':
      return isInLiteralValue(value)
        ? excludedColumnDomain(condition.left, [value])
        : undefined;
    case 'NOT IN':
      return Array.isArray(value) &&
        value.length > 0 &&
        value.every(isInLiteralValue)
        ? excludedColumnDomain(condition.left, value)
        : undefined;
    default:
      return undefined;
  }
}

function positiveColumnDomainFromCondition(
  condition: Condition,
): DomainPredicate<PositiveColumnDomain> | undefined {
  const predicate = columnDomainFromCondition(condition);
  if (!predicate || predicate.domain.include === undefined) {
    return undefined;
  }
  if (predicate.domain.exclude.length > 0) {
    return undefined;
  }

  return {
    domain: {
      left: predicate.domain.left,
      include: predicate.domain.include,
    },
    changed: predicate.changed,
  };
}

function finiteColumnDomain(
  left: ValuePosition,
  values: readonly InLiteralValue[],
): DomainPredicate<ColumnDomain> {
  const include = literalSet(values);
  return {
    domain: {left, include, exclude: []},
    changed: include.length !== values.length,
  };
}

function excludedColumnDomain(
  left: ValuePosition,
  values: readonly InLiteralValue[],
): DomainPredicate<ColumnDomain> {
  const exclude = literalSet(values);
  return {
    domain: {left, include: undefined, exclude},
    changed: exclude.length !== values.length,
  };
}

function intersectColumnDomains(
  left: ColumnDomain,
  right: ColumnDomain,
): ColumnDomain {
  return {
    left: left.left,
    include: intersectOptionalValueSets(left.include, right.include),
    exclude: unionLiteralValues(left.exclude, right.exclude),
  };
}

function unionPositiveColumnDomains(
  left: PositiveColumnDomain,
  right: PositiveColumnDomain,
): PositiveColumnDomain {
  return {
    left: left.left,
    include: unionLiteralValues(left.include, right.include),
  };
}

function columnDomainToCondition(domain: ColumnDomain): Condition {
  if (domain.include !== undefined) {
    return buildInCondition(
      domain.left,
      subtractLiteralValues(domain.include, domain.exclude),
    );
  }
  return buildNotInCondition(domain.left, domain.exclude);
}

function positiveColumnDomainToCondition(
  domain: PositiveColumnDomain,
): Condition {
  return buildInCondition(domain.left, domain.include);
}

function domainKey(domain: ColumnDomainBase): string {
  return stableStringify(domain.left);
}

function collapseImpossibleCorrelatedSubquery(
  condition: CorrelatedSubqueryCondition,
): Condition {
  if (condition.scalar === true) {
    return condition;
  }
  const {subquery} = condition.related;
  const subqueryCannotEmitRows =
    subquery.limit === 0 ||
    (subquery.where !== undefined && isAlwaysFalse(subquery.where));
  if (!subqueryCannotEmitRows) {
    return condition;
  }
  return condition.op === 'EXISTS' ? FALSE : TRUE;
}

function buildInCondition(
  left: ValuePosition,
  values: readonly InLiteralValue[],
): Condition {
  switch (values.length) {
    case 0:
      return FALSE;
    case 1:
      return {
        type: 'simple',
        left,
        op: '=',
        right: {type: 'literal', value: values[0]},
      };
    default:
      return {
        type: 'simple',
        left,
        op: 'IN',
        right: {type: 'literal', value: values},
      };
  }
}

function buildNotInCondition(
  left: ValuePosition,
  values: readonly InLiteralValue[],
): Condition {
  switch (values.length) {
    case 0:
      return TRUE;
    case 1:
      return {
        type: 'simple',
        left,
        op: '!=',
        right: {type: 'literal', value: values[0]},
      };
    default:
      return {
        type: 'simple',
        left,
        op: 'NOT IN',
        right: {type: 'literal', value: values},
      };
  }
}

function isInLiteralValue(
  value: LiteralValue | undefined,
): value is InLiteralValue {
  return value !== undefined && value !== null && !Array.isArray(value);
}

function dedupeInLiteralValues(
  values: readonly InLiteralValue[],
): InLiteralValue[] {
  return literalSet(values);
}

function literalSet(values: readonly InLiteralValue[]): InLiteralValue[] {
  const seen = new Set<string>();
  const deduped: InLiteralValue[] = [];
  for (const value of values) {
    const key = literalValueKey(value);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    deduped.push(value);
  }
  return deduped;
}

function intersectOptionalValueSets(
  left: readonly InLiteralValue[] | undefined,
  right: readonly InLiteralValue[] | undefined,
): readonly InLiteralValue[] | undefined {
  if (left === undefined) {
    return right;
  }
  if (right === undefined) {
    return left;
  }
  return intersectLiteralValues(left, right);
}

function unionLiteralValues(
  left: readonly InLiteralValue[],
  right: readonly InLiteralValue[],
): InLiteralValue[] {
  const seen = new Set(left.map(literalValueKey));
  const union = [...left];
  for (const value of right) {
    const key = literalValueKey(value);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    union.push(value);
  }
  return union;
}

function intersectLiteralValues(
  left: readonly InLiteralValue[],
  right: readonly InLiteralValue[],
): InLiteralValue[] {
  const rightValues = new Set(right.map(literalValueKey));
  return left.filter(value => rightValues.has(literalValueKey(value)));
}

function subtractLiteralValues(
  values: readonly InLiteralValue[],
  excluded: readonly InLiteralValue[],
): InLiteralValue[] {
  const excludedValues = new Set(excluded.map(literalValueKey));
  return values.filter(value => !excludedValues.has(literalValueKey(value)));
}

function literalValueKey(value: InLiteralValue): string {
  return stableStringify(value);
}

function flatten(
  type: 'and' | 'or',
  conditions: readonly Condition[],
): Condition[] {
  const flattened: Condition[] = [];
  for (const condition of conditions) {
    if (condition.type === type) {
      flattened.push(...condition.conditions);
    } else {
      flattened.push(condition);
    }
  }
  return flattened;
}

function factorCommonConjuncts(
  conditions: readonly Condition[],
): Condition | undefined {
  if (conditions.length < 2) {
    return undefined;
  }

  const branches = conditions.map(condition =>
    condition.type === 'and' ? [...condition.conditions] : [condition],
  );
  const commonKeys = new Set(branches[0].map(conditionKey));
  for (const branch of branches.slice(1)) {
    const branchKeys = new Set(branch.map(conditionKey));
    for (const key of [...commonKeys]) {
      if (!branchKeys.has(key)) {
        commonKeys.delete(key);
      }
    }
  }

  if (commonKeys.size === 0) {
    return undefined;
  }

  const common: Condition[] = [];
  const firstBranch = branches[0];
  for (const condition of firstBranch) {
    if (commonKeys.has(conditionKey(condition))) {
      common.push(condition);
    }
  }

  const remainingBranches = branches.map(branch =>
    branch.filter(condition => !commonKeys.has(conditionKey(condition))),
  );
  // Do not turn this:
  //
  //   A OR (A AND EXISTS(client_helper))
  //
  // into this:
  //
  //   A AND (TRUE OR EXISTS(client_helper))  ->  A
  //
  // The parent ids are the same, but the synced row set is not. The client
  // still needs the helper row that proved the EXISTS branch. Permission
  // helpers are private server evidence, so conditionContainsSyncedSubqueryEvidence
  // deliberately ignores them.
  if (
    remainingBranches.some(branch => branch.length === 0) &&
    remainingBranches.some(branch =>
      branch.some(conditionContainsSyncedSubqueryEvidence),
    )
  ) {
    return undefined;
  }

  return buildAnd([
    ...common,
    buildOr(remainingBranches.map(branch => buildAnd(branch))),
  ]);
}

function absorbRedundantOrBranches(
  conditions: readonly Condition[],
): Condition[] {
  const branches = conditions.map(condition => ({
    keys:
      condition.type === 'and'
        ? new Set(condition.conditions.map(conditionKey))
        : new Set([conditionKey(condition)]),
    syncedEvidenceKeys: syncedEvidenceKeys(condition),
  }));
  const keep = conditions.map(() => true);

  for (const [candidateIndex, candidate] of branches.entries()) {
    if (!keep[candidateIndex]) {
      continue;
    }
    for (const [branchIndex, branch] of branches.entries()) {
      if (candidateIndex === branchIndex || !keep[branchIndex]) {
        continue;
      }
      if (!branchSubsumes(candidate.keys, branch.keys)) {
        continue;
      }
      if (!canDropBranchWithoutLosingSyncedEvidence(branch, candidate)) {
        continue;
      }
      if (
        candidate.keys.size < branch.keys.size ||
        candidateIndex < branchIndex
      ) {
        keep[branchIndex] = false;
      }
    }
  }

  return conditions.filter((_, index) => keep[index]);
}

function canDropBranchWithoutLosingSyncedEvidence(
  branch: {
    readonly syncedEvidenceKeys: ReadonlySet<string>;
  },
  candidate: {
    readonly keys: ReadonlySet<string>;
  },
): boolean {
  // Boolean absorption is only row-set safe when every client helper row from
  // the branch being dropped is still represented by the branch that remains:
  //
  //   safe:     EXISTS(child) OR (EXISTS(child) AND A)  -> EXISTS(child)
  //   unsafe:   A OR (A AND EXISTS(child))              -> keep both
  //
  // Both expressions return the same parent ids, but only the unsafe original
  // carries the child helper rows the client needs after hydration.
  for (const key of branch.syncedEvidenceKeys) {
    if (!candidate.keys.has(key)) {
      return false;
    }
  }
  return true;
}

function branchSubsumes(
  candidate: ReadonlySet<string>,
  branch: ReadonlySet<string>,
): boolean {
  for (const key of candidate) {
    if (!branch.has(key)) {
      return false;
    }
  }
  return true;
}

function syncedEvidenceKeys(condition: Condition): ReadonlySet<string> {
  const conditions =
    condition.type === 'and' ? condition.conditions : [condition];
  return new Set(
    conditions
      .filter(conditionContainsSyncedSubqueryEvidence)
      .map(conditionKey),
  );
}

function conditionContainsSyncedSubqueryEvidence(
  condition: Condition,
): boolean {
  if (condition.type === 'correlatedSubquery') {
    return condition.related.system !== 'permissions';
  }
  if (condition.type === 'and' || condition.type === 'or') {
    return condition.conditions.some(conditionContainsSyncedSubqueryEvidence);
  }
  return false;
}

function mergeSameRelationshipExists(
  conditions: readonly Condition[],
): Condition[] {
  const merged: Array<Condition | undefined> = [...conditions];
  const groups = new Map<
    string,
    {
      readonly template: CorrelatedSubqueryCondition;
      readonly firstIndex: number;
      readonly filters: Condition[];
    }
  >();

  for (const [index, condition] of conditions.entries()) {
    const key = mergeableExistsKey(condition);
    if (!key || condition.type !== 'correlatedSubquery') {
      continue;
    }

    const group = groups.get(key);
    const filter = condition.related.subquery.where ?? TRUE;
    if (group) {
      merged[index] = undefined;
      group.filters.push(filter);
    } else {
      groups.set(key, {
        template: condition,
        firstIndex: index,
        filters: [filter],
      });
    }
  }

  for (const group of groups.values()) {
    merged[group.firstIndex] = mergeExistsGroup(group.template, group.filters);
  }

  return dedupe(
    merged.filter((condition): condition is Condition => !!condition),
  );
}

function mergeExistsGroup(
  template: CorrelatedSubqueryCondition,
  filters: readonly Condition[],
): Condition {
  const where = buildOr(filters);
  return collapseImpossibleCorrelatedSubquery({
    ...template,
    related: {
      ...template.related,
      subquery: {
        ...template.related.subquery,
        where: isAlwaysTrue(where) ? undefined : where,
      },
    },
  });
}

function mergeableExistsKey(condition: Condition): string | undefined {
  if (condition.type !== 'correlatedSubquery') {
    return undefined;
  }
  if (condition.op !== 'EXISTS' || condition.scalar === true) {
    return undefined;
  }

  const {related} = condition;
  const {subquery} = related;
  if (
    subquery.related !== undefined ||
    subquery.start !== undefined ||
    subquery.limit !== undefined
  ) {
    return undefined;
  }

  // EXISTS only asks whether at least one child row exists. Without related
  // rows, start, or limit, child ordering cannot affect that answer. Ignore
  // orderBy here so production callers that completed implicit primary-key
  // ordering before planning still get the same logical EXISTS merge.
  const mergeShape: CorrelatedSubquery = {
    ...related,
    subquery: {
      schema: subquery.schema,
      table: subquery.table,
      alias: subquery.alias,
    },
  };
  return stableStringify({
    op: condition.op,
    flip: condition.flip,
    scalar: condition.scalar,
    related: mergeShape,
  });
}

function dedupe(conditions: readonly Condition[]): Condition[] {
  const seen = new Set<string>();
  const deduped: Condition[] = [];
  for (const condition of conditions) {
    const key = conditionKey(condition);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    deduped.push(condition);
  }
  return deduped;
}

function conditionKey(condition: Condition): string {
  return stableStringify(condition);
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(',')}]`;
  }
  if (value && typeof value === 'object') {
    const entries = Object.entries(value)
      .filter(([, entry]) => entry !== undefined)
      .sort(([left], [right]) => left.localeCompare(right));
    return `{${entries
      .map(([key, entry]) => `${JSON.stringify(key)}:${stableStringify(entry)}`)
      .join(',')}}`;
  }
  return JSON.stringify(value) ?? 'undefined';
}

function isAlwaysTrue(condition: Condition): boolean {
  return condition.type === 'and' && condition.conditions.length === 0;
}

function isAlwaysFalse(condition: Condition): boolean {
  return condition.type === 'or' && condition.conditions.length === 0;
}
