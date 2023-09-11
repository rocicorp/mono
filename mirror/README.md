# Mirror

## Package / Directory Layout

- `mirror-cli`: cli for admin operations. Uses `gcloud` login (@roci.dev) for authorization
- `mirror-protocol`: JSON REST API to Cloud Functions
- `mirror-schema`: Firestore schema (Mirror database)
- `mirror-server`: Cloud Function implementation (Mirror server business logic)
- `reflect-cli`: User-level cli for `npx @rocicorp/reflect <command>` operations. Packaged by `packages/reflect`

## Mirror "Stacks"

A mirror "stack" is an isolated set of systems/accounts that constitute a
functional version of mirror. A stack consists of:

- A Firebase / GCP project, comprising
  - A Firestore database
  - Firebase Auth instance
  - A Google Cloud Storage bucket for storing javascript modules (server and app)
  - Cloud Functions that run the Mirror Server logic
  - Secret Manager for storing sensitive data like keys
- A Cloudflare account for running Workers and Durable Objects

(Note that the one part of Mirror stacks that are not isolated are the Datadog
sinks. We use a single Datadog account for logging / metrics of all stacks.)

We run two stacks:

- The `prod` stack is the public stack that services our customers and production apps.
- The `sandbox` stack is used in development. All major changes are run on `sandbox`
  before landing and deploying to `prod`. Breaking things in sandbox is fair game.

## Setting up a Stack

### Create a Firebase / GCP project

- Go to https://console.firebase.google.com/ and go through the "Add project" flow.
- The project should be named `reflect-mirror-{{stackname}}` to work with the
  naming logic in our cli and server code.

### Authentication

- In the new project, enable `Authentication` and setup a Github Authentication provider.
- Create and connect to a new OAuth app in the Github Rocicorp Organization:
  https://github.com/organizations/rocicorp/settings/applications

### Firestore

- In the new project, create the Firestore database.
- Add the project to `mirror/mirror-schema/.firebaserc`
- Add and run scripts to `mirror/mirror-schema/package.json` for deploying the security
  rules and indexes to Firestore. Then run them.

```sh
$ npm run deploy-rules-sandbox
$ npm run deploy-indexes-sandbox
```

### Google Cloud Storage

- Go to the Cloud Storage GCP dashboard at https://console.cloud.google.com/storage/browser?project=reflect-mirror-{{stackname}}
- Create a new bucket called `reflect-mirror-{{stackname}}-modules`

### Secret Manager

- Go to https://console.cloud.google.com/security/secret-manager?project=reflect-mirror-{{stackname}}
- Click `Enable API`
- That's it. You will enter secrets later.

### Service Accounts / IAM

We use one service account for running the Cloud Functions, and another for
running admin auth commands from the `mirror-cli`.

- Go to https://console.cloud.google.com/iam-admin/serviceaccounts?project=reflect-mirror-{{stackname}}
- Click `CREATE SERVICE ACCOUNT` and go through the creation flow for these two accounts:

  - Service Account ID: `functions`
  - Roles:

    - `Cloud Functions Service Agent`
    - `Editor`
    - `Service Account Token Creator`

  - Service Account ID: `super-granter`
  - Roles:
    - `Firebase Authentication Admin`
    - `Service Account Token Creator`

- Go to https://console.cloud.google.com/iam-admin/iam?project=reflect-mirror-{{stackname}}
- Click on `GRANT ACCESS` and grant:
  - Principal: `team@roci.dev`
  - Role: `Owner`
  - Role: `Service Account Token Creator`

### Cloudflare Account

A Cloudflare account is needed to run workers and Durable Objects.

- This must be a PAID account ($5/mo) in order to run Durable Objects.
- It must be configured with a domain name (e.g. sandbox uses `reflect-server.dev`)
  on which the workers are served.
- [Create an API key](https://dash.cloudflare.com/profile/api-tokens) for the Mirror Server
  (i.e. Cloud Functions) to make API calls. It should have the following permissions:
  - `Account: Workers Scripts`: Edit
  - `Account: Workers Tail`: Read
  - `Account: Account Analytics`: Read
  - `Zone: Workers Routes`: Edit
  - `Zone: SSL and Certificates`: Read
- In the Firestore console (https://console.firebase.google.com/project/reflect-mirror-{{stackname}}/firestore/data), create the document in a "cloudflares" collection:

```json
"cloudflares/<cloudflare-account-id>": {
  "defaultMaxApps": 5,
  "domain": "<the-domain-name-of-the-cloudflare-account>"
}
```

For example, the sandbox document is:

```json
"cloudflares/b50bbcc3bb7e6f0383c8048e1978c660": {
  "defaultMaxApps": 100,
  "domain": "reflect-server.dev"
}
```

### Cloud Functions

- Add the project to `mirror/mirror-server/.firebaserc`
- Add and run scripts to `mirror/mirror-server/package.json` for deploying the functions
  on the stack. Then run them.
- You will be prompted for the following parameters / secrets:
  - `DATADOG_LOGS_API_KEY`: Create a new one for this stack (e.g. "mirror-sandbox-logs") at https://app.datadoghq.com/organization-settings/api-keys
  - `DATADOG_METRICS_API_KEY`: Create one like you did for logs.
  - `CLOUDFLARE_API_TOKEN`: Use the API key that you created in the Cloudflare section
  - `CLOUDFLARE_ACCOUNT_ID`: The account ID of the Cloudflare account

* **Note**: One the first deploy, the deployment of Firestore Triggers usually fails,
  with a message to try again in a couple of minutes. This is normal and expected.

### Client Apps

- Go to the Firebase Project Overview for the project: https://console.firebase.google.com/project/reflect-mirror-{{stackname}}/overview
- Add two web apps:
  1. `reflect-auth-ui`
  2. `reflect-cli`
- Add the configuration of the first the auth login page at `apps/reflect.net/firebase-config/firebase.config.ts`
- Add the configuration of the second to the reflect-cli at `mirror/reflect-cli/src/firebase.ts`
- Also add the new stack name to the `--stack` value options in:
  - `mirror/mirror-cli/src/create-cli-parser.ts`
  - `mirror/reflect-cli/src/create-cli-parser.ts`

### LFG

That should be it. To give it a spin, you'll need to:

- Run the login page, either locally or on some Vercel deployment
- Run `reflect create --stack=stackname my-app` to create a new project
- Run `reflect publish --stack=stackname` which will go through the login, app creation, and publish step
