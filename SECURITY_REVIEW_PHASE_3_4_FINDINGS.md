# Security Review Phase 3.4 - Unicode Normalization

## Executive Summary

This document contains the security review findings for Phase 3.4 (Unicode Normalization), focusing on potential homoglyph confusion attacks due to missing NFC normalization on identifiers.

**Overall Assessment**: 1 low-severity issue identified. The risk is mitigated by the fact that most identifiers either use restricted character sets or are generated server-side.

---

## Background: Unicode Homoglyph Attacks

Unicode homoglyphs are characters that look identical or similar but have different code points. For example:
- Latin 'a' (U+0061) vs Cyrillic 'а' (U+0430)
- Latin 'e' (U+0065) vs Cyrillic 'е' (U+0435)
- Latin 'o' (U+006F) vs Cyrillic 'о' (U+043E)

Without Unicode normalization (NFC), these characters are treated as different, which can lead to:
1. **Impersonation**: User "admin" vs "аdmin" (with Cyrillic 'а')
2. **Permission bypass**: Table "users" vs "usеrs" (with Cyrillic 'е')
3. **Data confusion**: Records appearing identical but being different

---

## Findings

### LOW-01: No Unicode Normalization on User-Controlled Identifiers

**Severity**: LOW
**CVSS Score**: 3.7 (Low)
**Files**:
- `packages/zero-protocol/src/push.ts`
- `packages/zero-protocol/src/connect.ts`
- `packages/zero-cache/src/auth/read-authorizer.ts`
- `packages/zero-cache/src/auth/write-authorizer.ts`

**Vulnerability Description**:

The codebase does not perform Unicode NFC normalization on identifiers before comparison or storage. All string identifiers use simple `v.string()` validation without normalization:

```typescript
// push.ts - No normalization on tableName
const insertOpSchema = v.object({
  op: v.literal('insert'),
  tableName: v.string(),  // Accepts any Unicode string
  primaryKey: primaryKeySchema,
  value: rowSchema,
});

// connect.ts - No normalization on client identifiers
const cleanupResultsArgSchema = v.object({
  clientGroupID: v.string(),  // Accepts any Unicode string
  clientID: v.string(),
  upToMutationID: v.number(),
});
```

**Affected Identifiers**:

| Identifier | Source | Validated | Risk |
|------------|--------|-----------|------|
| App ID | Config | `[a-z0-9_]+` | **NONE** - ASCII only |
| Table Name | Client | `v.string()` | LOW - Must match schema |
| Column Name | Client | `v.string()` | LOW - Must match schema |
| Client ID | Client | `v.string()` | LOW - Self-referential |
| Client Group ID | Client | `v.string()` | LOW - Self-referential |
| User ID (JWT sub) | Auth Provider | `v.string()` | **MEDIUM** - Used in authz |
| Query ID | Client | Generated hash | **NONE** - Computed |

**Impact Analysis**:

1. **Table/Column Names**: Low risk because names must match the actual database schema. A homoglyph attack would simply fail to find the table.

2. **Client/ClientGroup IDs**: Low risk because these are primarily self-referential - a client uses its own IDs to manage its own state.

3. **User IDs (JWT `sub`)**: Medium risk in specific scenarios:
   ```typescript
   // read-authorizer.ts:66
   let rowSelectRules = permissionRules?.tables?.[query.table]?.row?.select;

   // Used in permission rules like:
   cmp('ownerId', '=', authData.sub)
   ```
   If a JWT contains a homoglyph user ID, it won't match database records with the ASCII version.

**Attack Scenario**:

Consider this permission rule:
```typescript
// Permission: Users can only see their own data
(authData, eb) => eb.cmp('ownerId', '=', authData.sub)
```

Scenario:
1. Database stores: `ownerId = "user1"` (ASCII)
2. Attacker's JWT has: `sub: "usеr1"` (Cyrillic 'е')
3. Permission check: `"user1" === "usеr1"` → FALSE
4. Result: Attacker cannot access data (secure by accident)

However, the reverse is also possible:
1. Attacker creates account with homoglyph ID
2. Database stores: `ownerId = "usеr1"` (Cyrillic 'е')
3. Legitimate user with ASCII ID cannot access this data
4. Result: Confusion, potential data hiding

---

## Positive Findings

### App ID - Properly Restricted

**Status**: SECURE
**File**: `packages/zero-cache/src/types/shards.ts`

App IDs are restricted to ASCII alphanumeric and underscore:

```typescript
export const ALLOWED_APP_ID_CHARACTERS = /^[a-z0-9_]+$/;

export const INVALID_APP_ID_MESSAGE =
  'The App ID may only consist of lower-case letters, numbers, and the underscore character';

export function check(shard: ShardID): {appID: string; shardNum: number} {
  const {appID, shardNum} = shard;
  if (!ALLOWED_APP_ID_CHARACTERS.test(appID)) {
    throw new Error(INVALID_APP_ID_MESSAGE);
  }
  // ...
}
```

This completely prevents Unicode homoglyph attacks on app identifiers.

### Query/Transformation Hashes - Computed

**Status**: SECURE
**File**: `packages/zero-protocol/src/query-hash.ts`

Query and transformation identifiers are computed hashes, not user-provided strings. This makes homoglyph attacks irrelevant for these identifiers.

---

## Risk Assessment

### Why This Is Low Severity

1. **Schema Validation**: Table and column names must match actual schema definitions, which are defined by developers (not end users).

2. **Self-Referential**: Client IDs primarily reference the client's own data. A client using homoglyphs only affects itself.

3. **Auth Provider Responsibility**: User IDs come from external auth providers (JWT issuers), who are responsible for normalizing identifiers.

4. **Fail-Closed**: Homoglyph mismatches typically cause permission denials (secure by default) rather than grants.

### When This Could Be Higher Severity

1. **User-Generated Content**: If usernames/display names are used in permission rules.

2. **Multi-Tenant Confusion**: If tenant IDs allow Unicode and are used for data isolation.

3. **Audit/Display**: If identifiers are displayed to humans who might be confused by homoglyphs.

---

## Recommendations

### Option 1: Add NFC Normalization (Recommended)

Add normalization at the validation layer:

```typescript
import * as v from 'valita';

const normalizedString = () =>
  v.string().map(s => s.normalize('NFC'));

// Use in schemas:
const insertOpSchema = v.object({
  op: v.literal('insert'),
  tableName: normalizedString(),
  primaryKey: primaryKeySchema,
  value: rowSchema,
});
```

### Option 2: Restrict to ASCII for Critical Identifiers

For security-critical identifiers, restrict to ASCII:

```typescript
const ASCII_IDENTIFIER = /^[a-zA-Z0-9_-]+$/;

const safeIdentifier = () =>
  v.string().assert(
    s => ASCII_IDENTIFIER.test(s),
    'Identifier must be ASCII alphanumeric, underscore, or hyphen'
  );
```

### Option 3: Document Auth Provider Requirements

Document that auth providers should normalize user identifiers (JWT `sub` claim) to NFC before signing tokens.

---

## Implementation Priority

| Recommendation | Effort | Impact | Priority |
|---------------|--------|--------|----------|
| Document auth provider requirements | Low | Medium | P2 |
| Add NFC normalization to validators | Medium | Low | P3 |
| Restrict critical identifiers to ASCII | Medium | Low | P3 |

---

## Test Cases

### Test 1: Homoglyph Table Name Rejection
```typescript
test('rejects table names with homoglyphs when schema uses ASCII', async () => {
  const op = {
    op: 'insert',
    tableName: 'usеrs',  // Cyrillic 'е'
    primaryKey: ['id'],
    value: { id: '1' }
  };

  // Should fail because 'usеrs' doesn't match schema's 'users'
  await expect(processOp(op)).rejects.toThrow(/table.*not found/i);
});
```

### Test 2: Homoglyph User ID in Permission Check
```typescript
test('homoglyph user IDs do not match ASCII IDs', async () => {
  // Database has ASCII user
  await db.insert('users', { id: 'user1', name: 'Test' });

  // JWT with Cyrillic 'е' in sub
  const authData = { sub: 'usеr1' };

  // Permission rule: cmp('id', '=', authData.sub)
  const result = await query('users').where('id', '=', authData.sub).run();

  // Should return empty - no match
  expect(result).toHaveLength(0);
});
```

### Test 3: NFC Normalization
```typescript
test('NFC normalization makes composed/decomposed forms equivalent', () => {
  // 'é' as single code point vs 'e' + combining acute
  const composed = '\u00e9';           // é (NFC)
  const decomposed = '\u0065\u0301';   // e + ́ (NFD)

  expect(composed).not.toBe(decomposed);
  expect(composed.normalize('NFC')).toBe(decomposed.normalize('NFC'));
});
```

---

## Files Reviewed

| File | Purpose | Findings |
|------|---------|----------|
| `packages/zero-cache/src/types/shards.ts` | App ID validation | ASCII-only (secure) |
| `packages/zero-protocol/src/push.ts` | Push/mutation schemas | No normalization |
| `packages/zero-protocol/src/connect.ts` | Connection schemas | No normalization |
| `packages/zero-cache/src/auth/read-authorizer.ts` | Read permission checks | String comparison |
| `packages/zero-cache/src/auth/write-authorizer.ts` | Write permission checks | String comparison |
| `packages/zql/src/builder/builder.ts` | Query builder | Uses authData directly |

---

## Appendix: Unicode Normalization Forms

| Form | Description | Use Case |
|------|-------------|----------|
| NFC | Canonical Decomposition, followed by Canonical Composition | Default for most applications |
| NFD | Canonical Decomposition | String analysis |
| NFKC | Compatibility Decomposition, followed by Canonical Composition | Search/matching |
| NFKD | Compatibility Decomposition | Search/matching |

**Recommendation**: Use NFC for identifier normalization. It's the W3C recommended form and produces the most compact representation.

---

## Appendix: Common Homoglyph Pairs

| ASCII | Lookalike | Unicode Name |
|-------|-----------|--------------|
| a | а | CYRILLIC SMALL LETTER A (U+0430) |
| c | с | CYRILLIC SMALL LETTER ES (U+0441) |
| e | е | CYRILLIC SMALL LETTER IE (U+0435) |
| o | о | CYRILLIC SMALL LETTER O (U+043E) |
| p | р | CYRILLIC SMALL LETTER ER (U+0440) |
| x | х | CYRILLIC SMALL LETTER HA (U+0445) |
| y | у | CYRILLIC SMALL LETTER U (U+0443) |
| A | А | CYRILLIC CAPITAL LETTER A (U+0410) |
| B | В | CYRILLIC CAPITAL LETTER VE (U+0412) |
| E | Е | CYRILLIC CAPITAL LETTER IE (U+0415) |
| H | Н | CYRILLIC CAPITAL LETTER EN (U+041D) |
| K | К | CYRILLIC CAPITAL LETTER KA (U+041A) |
| M | М | CYRILLIC CAPITAL LETTER EM (U+041C) |
| O | О | CYRILLIC CAPITAL LETTER O (U+041E) |
| P | Р | CYRILLIC CAPITAL LETTER ER (U+0420) |
| T | Т | CYRILLIC CAPITAL LETTER TE (U+0422) |
| X | Х | CYRILLIC CAPITAL LETTER HA (U+0425) |
