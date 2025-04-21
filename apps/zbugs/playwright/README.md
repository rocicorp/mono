# Running playwright tests locally

- Add ZERO_AUTH_JWK. Set this to the public key that is output by `npm run create-keys` in `zbugs`

## Push docker container for AWS batch

- docker build --platform linux/amd64 -t roci-zero-loadtest .
- docker tag roci-zero-loadtest:latest <accid>.dkr.ecr.us-east-1.amazonaws.com/roci-zero-loadtest:latest
- docker push <accid>.dkr.ecr.us-east-1.amazonaws.com/roci-zero-loadtest:latest

- URL=https://bugs-sandbox.rocicorp.dev ADD_COMMENTS_AND_EMOJI=1 ISSUE_ID=3020 ENTER_PASSWORD=0 npx playwright test --ui
  TODO: You are supposed to be able to run in a real browser and debug by
  replacing --ui with --headed, but this doesn't work for me. I get:

TypeError: Cannot redefine property: Symbol($$jest-matchers-object)
at /Users/aa/work/mono/node_modules/@vitest/expect/dist/index.js:589:10
