# Running playwright tests locally

- Add ZERO_AUTH_JWK. Set this to the public key that is output by `npm run create-keys` in `zbugs`
- URL="http://localhost:5174" PERCENT_DIRECT=1 npx playwright test --ui

TODO: You are supposed to be able to run in a real browser and debug by
replacing --ui with --headed, but this doesn't work for me. I get:

TypeError: Cannot redefine property: Symbol($$jest-matchers-object)
at /Users/aa/work/mono/node_modules/@vitest/expect/dist/index.js:589:10
