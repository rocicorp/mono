# Security Review Phase 2.3 - Prototype Pollution

## Executive Summary

This document contains the security review findings for Phase 2.3 (Prototype Pollution):
- **2.3 Prototype Pollution** (zero-server)

**Overall Assessment**: 1 medium-severity issue and 1 low-severity issue identified. No critical prototype pollution vulnerabilities found. The codebase has existing mitigations that prevent the most dangerous attack vectors.

---

## Findings

### MEDIUM-01: Prototype Chain Traversal in Mutator Resolution

**Severity**: MEDIUM
**CVSS Score**: 4.3 (Medium)
**Files**:
- `packages/shared/src/object-traversal.ts:30-45`
- `packages/zero-server/src/push-processor.ts:97-114`

**Vulnerability Description**:

The `getValueAtPath()` function uses the `in` operator to check property existence, which traverses the prototype chain:

```typescript
// object-traversal.ts:30-45
export function getValueAtPath(
  obj: object,
  path: string,
  sep: string | RegExp,
): unknown {
  const parts = path.split(sep);
  let current: unknown = obj;
  for (const part of parts) {
    if (current && typeof current === 'object' && part in current) {  // <-- VULNERABLE
      current = (current as Record<string, unknown>)[part];
    } else {
      return undefined;
    }
  }
  return current;
}
```

This is called from `push-processor.ts`:

```typescript
// push-processor.ts:104
const mutator = getValueAtPath(mutators, key, /\.|\|/);
```

**Attack Vectors**:

| Mutation Name | Result | Impact |
|--------------|--------|--------|
| `"constructor"` | Returns `Object` constructor | Calls `Object(dbTx, args)` - boxes transaction |
| `"__proto__"` | Returns `Object.prototype` | Assertion failure (not a function) |
| `"toString"` | Returns `Object.prototype.toString` | Calls `toString.call(undefined, args)` |
| `"hasOwnProperty"` | Returns `Object.prototype.hasOwnProperty` | Calls with wrong `this` binding |
| `"constructor.prototype"` | Returns `Object.prototype` | Assertion failure (not a function) |

**Proof of Concept**:

```json
{
  "mutations": [{
    "type": "custom",
    "clientID": "attacker",
    "id": 1,
    "name": "constructor",
    "args": [{"malicious": "data"}],
    "timestamp": 0
  }]
}
```

**Impact Analysis**:

1. **Low Impact - Calling `Object()` constructor**:
   - `Object(dbTx, args)` returns a boxed version of `dbTx`
   - No persistent side effects
   - Transaction is not corrupted

2. **Low Impact - Calling inherited methods**:
   - Methods like `toString` are called with incorrect `this` binding
   - Return values are typically strings or booleans
   - No database modifications occur

3. **Information Disclosure**:
   - Error messages may reveal internal type information
   - Timing differences could indicate which properties exist

**Existing Mitigations**:

1. **Function Type Check** (push-processor.ts:105):
   ```typescript
   assert(typeof mutator === 'function', `could not find mutator ${key}`);
   ```
   This prevents accessing non-function prototype properties like `__proto__`.

2. **Mutator Marker Check** (push-processor.ts:106):
   ```typescript
   if (isMutator(mutator)) {
     return mutator.fn({...});
   }
   ```
   This checks for `mutatorName` and `fn` properties, which built-in functions lack.

3. **No Prototype Modification**:
   The code only reads from the mutators object, never writes to it, so actual prototype pollution (modifying `Object.prototype`) is not possible.

**Recommendation**:

1. **Use `Object.hasOwn()` instead of `in` operator**:
```typescript
export function getValueAtPath(
  obj: object,
  path: string,
  sep: string | RegExp,
): unknown {
  const parts = path.split(sep);
  let current: unknown = obj;
  for (const part of parts) {
    if (current && typeof current === 'object' && Object.hasOwn(current, part)) {
      current = (current as Record<string, unknown>)[part];
    } else {
      return undefined;
    }
  }
  return current;
}
```

2. **Add explicit blocklist for dangerous names**:
```typescript
const BLOCKED_NAMES = new Set([
  '__proto__',
  'constructor',
  'prototype',
  'toString',
  'valueOf',
  'hasOwnProperty',
  'isPrototypeOf',
  'propertyIsEnumerable',
  'toLocaleString',
]);

if (BLOCKED_NAMES.has(part)) {
  return undefined;
}
```

3. **Use null-prototype objects for mutators** (like `create-builder.ts` does):
```typescript
const safeMutators = Object.assign(Object.create(null), mutators);
```

---

### LOW-01: No Validation on Mutation Name Format

**Severity**: LOW
**CVSS Score**: 3.1 (Low)
**File**: `packages/zero-protocol/src/push.ts:91-98`

**Vulnerability Description**:

The mutation name schema accepts any string without format validation:

```typescript
export const customMutationSchema = v.object({
  type: v.literal(MutationType.Custom),
  id: v.number(),
  clientID: v.string(),
  name: v.string(),  // <-- No format validation
  args: v.array(jsonSchema),
  timestamp: v.number(),
});
```

**Impact**:
- Mutation names with special characters are accepted
- Unicode homoglyphs could cause confusion
- Extremely long names could cause log injection or DoS

**Recommendation**:

Add format validation for mutation names:

```typescript
const mutationNameSchema = v.string()
  .map(s => {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/.test(s)) {
      throw new Error('Invalid mutation name format');
    }
    if (s.length > 256) {
      throw new Error('Mutation name too long');
    }
    return s;
  });

export const customMutationSchema = v.object({
  // ...
  name: mutationNameSchema,
  // ...
});
```

---

## Security Strengths Observed

### Existing Prototype Pollution Mitigations (GOOD)

The codebase shows awareness of prototype pollution in other areas:

1. **Null-prototype objects in create-builder.ts:36-43**:
```typescript
// Create a target with no prototype so accessing unknown properties returns
// undefined instead of inherited Object.prototype methods (e.g., toString).
const target = Object.assign(Object.create(null), schema.tables);
```

2. **Use of `Object.hasOwn()` in other files**:
- `packages/shared/src/has-own.ts` exports `Object.hasOwn`
- Used in `json.ts`, `deep-clone.ts`, `size-of-value.ts`

3. **Function type assertion**:
```typescript
assert(typeof mutator === 'function', `could not find mutator ${key}`);
```

### Mutator Validation (GOOD)

The `isMutator()` check provides defense in depth:

```typescript
export function isMutator<S extends Schema>(value: unknown): value is Mutator<...> {
  return (
    typeof value === 'function' &&
    typeof (value as {mutatorName?: unknown}).mutatorName === 'string' &&
    typeof (value as {fn?: unknown}).fn === 'function'
  );
}
```

This ensures only properly defined mutators with the marker properties are executed through the primary path.

---

## Remediation Priority

| Finding | Severity | Effort | Priority |
|---------|----------|--------|----------|
| MEDIUM-01: Prototype chain traversal | Medium | Low | P2 |
| LOW-01: No mutation name validation | Low | Low | P3 |

---

## Recommended Test Cases

### For MEDIUM-01 (Prototype Traversal):
```typescript
test('rejects mutation names that resolve to prototype properties', async () => {
  const processor = new PushProcessor(new FakeDatabase());
  const mutators = { valid: { mutator: async () => {} } };

  // These should not resolve to functions
  for (const name of ['__proto__', 'constructor', 'toString', 'hasOwnProperty']) {
    const result = await processor.process(
      mutators,
      testParams,
      makeMutationBody(name),
    );
    expect(result).toMatchObject({
      kind: ErrorKind.PushFailed,
      message: expect.stringContaining('could not find mutator'),
    });
  }
});

test('constructor mutation does not call Object constructor', async () => {
  const processor = new PushProcessor(new FakeDatabase());
  const mutators = {};

  const result = await processor.process(
    mutators,
    testParams,
    makeMutationBody('constructor'),
  );

  // Should fail assertion, not call Object()
  expect(result).toMatchObject({
    kind: ErrorKind.PushFailed,
  });
});
```

### For LOW-01 (Name Validation):
```typescript
test('rejects mutation names with invalid characters', async () => {
  const processor = new PushProcessor(new FakeDatabase());
  const mutators = { valid: async () => {} };

  for (const name of ['../escape', '<script>', 'name\ninjection']) {
    const result = await processor.process(
      mutators,
      testParams,
      makeMutationBody(name),
    );
    expect(result).toMatchObject({
      kind: ErrorKind.PushFailed,
    });
  }
});
```

---

## Files Reviewed

| File | Purpose | Findings |
|------|---------|----------|
| `packages/zero-server/src/push-processor.ts` | Mutation dispatch | Uses getValueAtPath |
| `packages/shared/src/object-traversal.ts` | Path traversal utility | Uses `in` operator |
| `packages/zql/src/mutate/mutator.ts` | Mutator type definitions | isMutator check |
| `packages/zero-protocol/src/push.ts` | Message schemas | No name validation |
| `packages/shared/src/has-own.ts` | Object.hasOwn export | Not used in traversal |
| `packages/zql/src/query/create-builder.ts` | Query builder | Good null-proto pattern |

---

## Comparison with Known Prototype Pollution Patterns

### Not Vulnerable To:

1. **Classic Prototype Pollution** (modifying `Object.prototype`):
   - No `obj[key] = value` patterns with user input
   - Only reading from objects, not writing

2. **JSON.parse Pollution**:
   - V8's JSON.parse is immune to `__proto__` pollution
   - JSON.parse output is always safe

3. **Lodash-style Deep Merge**:
   - No deep merge operations on user-controlled paths

### Partially Vulnerable To:

1. **Prototype Chain Traversal**:
   - Can read inherited properties
   - Mitigated by function type check
   - Recommended fix: Use `Object.hasOwn()`

---

## Appendix: JavaScript Prototype Chain Behavior

```javascript
const obj = { foo: { bar: () => {} } };

// in operator traverses prototype chain
'constructor' in obj  // true (inherited from Object.prototype)
'__proto__' in obj    // true (inherited)
'foo' in obj          // true (own property)

// Object.hasOwn only checks own properties
Object.hasOwn(obj, 'constructor')  // false
Object.hasOwn(obj, '__proto__')    // false
Object.hasOwn(obj, 'foo')          // true
```
