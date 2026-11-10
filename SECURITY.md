# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately. Do not open a public issue.

- Preferred: [open a private security advisory](https://github.com/rocicorp/mono/security/advisories/new).
- By email: arv@roci.dev.

Useful things to include: the affected package and version, what an attacker
can do, and the smallest set of steps, patch or test that shows the problem.
If a reproduction needs a running `zero-cache`, say how it is configured.

We will acknowledge your report, tell you whether we consider it a
vulnerability, and keep you posted while we work on a fix. We will credit you
in the advisory and the release notes unless you would rather we did not.

## Supported versions

Security fixes land in the latest stable `@rocicorp/zero` minor release line
and in canary. Earlier minors are not patched; please upgrade.

## Scope

`zero-cache` is run by each app on its own infrastructure and accepts
WebSocket connections from untrusted browser clients, so everything a client
sends is attacker-controlled. Reports we are most interested in:

- authentication or authorization bypass, including permission rules that are
  not applied
- one user or client group reading or writing another's data
- SQL injection, or remote code execution anywhere
- reaching the admin endpoints without `ZERO_ADMIN_PASSWORD`

Out of scope: anything that needs control of the operator's configuration,
environment, Postgres database or API server; missing rate limiting on its
own; and expensive queries an app has explicitly allowed.
