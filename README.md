# Replicache Quick Start

This document walks you through a simple [Replicache](https://replicache.dev) integration. Specifically, we're going to be building an offline-first Todo app. You can use this as a basis for adding Replicache to your own service.

Though simple, this sample app includes the main challenges common to SaaS applications that want to add offline-first: it's a multitenant web service, where users are authenticated and can share data with each other. It's already written in a classic JSON/REST architecture that we don't want to change, and it has its own backend storage that needs to remain authoratative. 

This walkthrough will show how Replicache makes it easy to add offline-first to these types of apps. It should take a few hours to work through, and another half day to a day or so to start to apply to your own system.

Questions? Comments? We'd love to help you evaluate Replicache — [Join us on Slack](https://join.slack.com/t/rocicorp/shared_invite/zt-ekh3oxbq-FzzB7qP9lwqdTvBjxXU2oA). 
You can also refer to our fully-functional [TODO sample application](https://github.com/rocicorp/replicache-sample-todo). For information about contributing, see our [contributing guide](contributing.md).

**Note:** This document assumes you already know what Replicache is, why you might need it, and broadly how it works. If that's not true, see the [Replicache homepage](https://replicache.dev) for an overview, or the [design document](design.md) for a detailed deep-dive.

# Overview

![Picture of Replicache Architecture](diagram.png)

Replicache is a per-user cache that sits between your backend and client. To integrate Replicache, you will make changes to both your backend and your client.

# Client Side

On the client side, you'll link the Replicache Client SDK into your application and use it as your local storage layer.

Currently we only support web/js clients. See the [Replicache JS SDK](https://github.com/rocicorp/replicache-sdk-js) repo for the client-side setup instructions.

# Server Side

On the server-side you will add two custom endpoints that Replicache will call to synchronize with your service.

### Step 1: Downstream Sync

Implement a *Client View* endpoint on your service that returns the data that should be available locally on the client for each user. This endpoint should return the *entire* view every time it is requested.

Replicache will frequently query this endpoint and calculate a diff to send to each client.

The format of the Client View is JSON that matches the following [JSON Schema](https://json-schema.org/):

```jsonschema
{
  "type": "object",
  "properties": {
    // The last Replicache Mutation ID that your service has processed. See Step 4 and 5 for more information.
    "lastMutationID": {"type": "integer", "minimum": 0},

    // An arbitrary map of key/value pairs. Any JSON type is legal for each value.
    // This is the data that will be available on the client side.
    "clientView": {"type": "object"}
  },
  "required": ["lastMutationID", "clientView"]
}
```

For example, [sample TODO app](https://github.com/rocicorp/replicache-sample-todo) returns a Client View like this:

```jsonc
{
  "lastMutationID": 0,
  "clientView": {
    "/todo/1": {
      "title": "Take out the trash",
      "description": "Don't forget to pull it to the curb.",
      "done": false,
    },
    "/todo/2": {
      "title": "Pick up milk",
      "description": "2%",
      "done": true,
    },
    ...
  }
}
```

The key/value pairs you return are up to you — Replicache just makes sure they get to the client.

#### Authentication

Most applications return a Client View that is specific to the calling user. Replicache supports sending user credentials through the standard `Authorization` HTTP header. If authorization fails, the Client View should return HTTP 401 (Unauthorized). The Replicache client will prompt user code to reauthenticate in that case.

#### Errors

All responses other than HTTP 200 with a valid JSON Client View and HTTP 401 are treated as errors by the Diff Server. The Client View response is ignored and the app is sent the last known state instead.

### Step 2: Test Downstream Sync

Download diff-server:

```bash
# For OSX
curl -o diffs -L https://github.com/rocicorp/diff-server/releases/latest/download/diffs-osx
chmod u+x diffs

# For Linux
curl -o diffs -L https://github.com/rocicorp/diff-server/releases/latest/download/diffs-linux
chmod u+x diffs
```

Run it:

```bash
# The --client-view parameter should point to the Client View endpoint you implemented above.
./<your-platform>/diffs --db=/tmp/foo serve \
  --client-view=http://localhost:8000/replicache-client-view
```

Then *pull* from that diff-server:

```bash
curl -H "Authorization:sandbox" -d '{"clientID":"c1", "baseStateID":"00000000000000000000000000000000", "checksum":"00000000"}' \
http://localhost:7001/pull
```

Take note of the returned `stateID` and `checksum`. Then make a change to your server and pull again, but specifying a `baseStateID` and `checksum` like so:


```bash
BASE_STATE_ID=<stateid-from-previous-response>
CHECKSUM=<checksum-from-previous-reseponse>
curl -H "Authorization:sandbox" -d '{"clientID":"c1", "baseStateID":"$BASE_STATE_ID", "checksum":"$CHECKSUM"}' \
http://localhost:7001/pull
```

You'll get a response that includes only the diff!

### Step 3: Mutation ID Storage

Next up: Writes.

Replicache identifies each change (or *Mutation*) that originates on the Replicache Client with a *MutationID*.

Your service must store the last MutationID it has processed for each client, and return it to Replicache in the Client View response. This allows Replicache to know when it can discard the speculative version of that change from the client.

Depending on how your service is built, storing MutationIDs can be done a number of ways. But typically you'll store them in the same database as your user data and update them transactionally as part of each mutation.

If you use e.g., Postgres, for your user data, you might store Replicache Change IDs in a table like:

<table>
  <tr>
    <th colspan=2>ReplicacheMutationIDs</th>
  </tr>
  <tr>
    <td>ClientID</td>
    <td>CHAR(32)</td>
  </tr>
  <tr>
    <td>LastMutationID</td>
    <td>INT64</td>
  </tr>
</table>

### Step 4: Upstream Sync

Replicache implements upstream sync by queuing calls to your service on the client-side and uploading them in batches. By default Replicache posts these batches to `https://yourdomain.com/replicache-batch`.

The payload of the batch request is JSON matching the JSON Schema:

```json
{
  "type": "object",
  "properties": {
    "clientID": {
      "type": "string",
      "minLength": 1
    },
    "mutations": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "id": {
            "type": "integer",
            "minimum": 1
          },
          "name": {
            "type": "string",
            "minLength": 1
          },
          "args": {},
        },
        "required": ["id", "name", "args"]
      }
    }
  },
  "required": ["clientID", "mutations"]
}
```

Here is an example batch request to our TODO example app backend.

```json
{
  "clientID": "CB94867E-94B7-48F3-A3C1-287871E1F7FD",
  "mutations": [
    {
      "id": 7,
      "name": "todoCreate",
      "args": {
        "id": "AE2E880D-C4BD-473A-B5E0-29A4A9965EE9",
        "text": "Take out the trash",
        "order": 0.5,
        "complete": false
      }
    },
    {
      "id": 8,
      "name": "todoUpdate",
      "args": {
        "id": "AE2E880D-C4BD-473A-B5E0-29A4A9965EE9",
        "complete": true
      }
    },
    ...
  ]
}
```

The response format matches the schema:

```json
{
  "type": "array",
  "properties": {
    "mutationInfos": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "id": {
            "type": "int",
            "minimium": 0,
          },
          "error": {
            "type": "string"
          }
        },
        "required": ["id"]
      }
    }
  },
  "required": ["mutationInfos"]
```

For example:

```json
{
  "mutationInfos": [
    {
      "id": 8,
      "error": "Invalid POST data: syntax error: ..."
    },
    {
      "id": 9,
      "error": "Backend unavailable"
    }
  ]
}
```

You do not ever need to return any `mutationInfos`. They are **completely informational** and not used programmatically by Replicache. However, Replicache does dump this response to the developer console for debugging purposes.

#### Implementing the Batch Endpoint

Conceptually, the batch endpoint receives an ordered batch of mutation requests and applies them in sequence, reporting any errors back to the client. There are some sublteties to be aware of, though:

* Replicache can send mutations that have already been processed. This is in fact common when the network is spotty. This is why you need to [store the last processed mutation ID](#step-4-mutation-id-storage): You **MUST** skip mutations you have already seen.
* Generally, mutations for a given client **SHOULD** be processed serially and in-order to achieve [causal consistency](https://jepsen.io/consistency/models/causal). However if you have special knowledge that pairs of mutations are commutative, you can process them in parallel.
* Each mutation **MUST** eventually be acknowledged by your service, by updating the stored `lastMutationID` value for the client and returning it in the client view.
  * If a mutation can't be processed temporarily (e.g., some server-side resource is temporarily unavailable), simply return early from the batch without updating `lastMutationID`. Replicache will retry the mutation later.
  * If a mutation can't be processed permanently (e.g., the request is invalid), mark the mutation processed by updating the stored `lastMutationID`, then continue with other mutations.
* You **MUST** update `lastMutationID` atomically with handling the mutation, otherwise the state reported to the client can be inconsistent.

A sample batch endpoint for Go is available in our [TODO sample app](https://github.com/rocicorp/replicache-sample-todo/blob/master/serve/handlers/batch/batch.go).

### Step 5: Example

Here's a bash transcript demonstrating a series of requests Replicache might make against our [sample TODO app](https://github.com/rocicorp/replicache-sample-todo):

```bash
BATCH=https://replicache-sample-todo.now.sh/serve/replicache-batch
CLIENT_VIEW=https://replicache-sample-todo.now.sh/serve/replicache-client-view
NEW_USER_EMAIL=$RANDOM@foo.com
PLATFORM=darwin-amd64 # or linux-amd64

# Start diffs talking to TODO service
./$PLATFORM/diffs --db=/tmp/foo serve --client-view=$CLIENT_VIEW &

# Create a new user
curl -d "{\"email\":\"$NEW_USER_EMAIL\"}" https://replicache-sample-todo.now.sh/serve/login

USER_ID=<user-id-from-prev-cmd>
CLIENT_ID=$RANDOM
LIST_ID=$RANDOM
TODO_ID=$RANDOM

# Create a first list and todo
curl -H "Authorization:$USER_ID" 'https://replicache-sample-todo.now.sh/serve/replicache-batch' --data-binary @- << EOF
{
    "clientID": "$CLIENT_ID",
    "mutations": [
        {
            "id": 1,
            "name": "createList",
            "args": {
                "id": $LIST_ID
            }
        },
        {
            "id": 2,
            "name": "createTodo",
            "args": {
                "id": $TODO_ID,
                "listID": $LIST_ID,
                "text": "Walk the dog",
                "order": 0.5,
                "complete": false
            }
        }
    ]
}
EOF

# Do an initial pull from diff-server
curl -H "Authorization:sandbox" http://localhost:7001/pull --data-binary @- << EOF
{
  "clientID":"$CLIENT_ID",
  "baseStateID":"00000000000000000000000000000000",
  "checksum":"00000000",
  "clientViewAuth": "$USER_ID"
}
EOF

BASE_STATE_ID=<stateID from prev response>
CHECKSUM=<checksum from prev response>

# Create a second TODO
# Do this one via the classic REST API, circumventing Replicache entirely
TODO_ID=$RANDOM
curl -H "Authorization:$USER_ID" https://replicache-sample-todo.now.sh/serve/todo-create --data-binary @- << EOF
{
  "id": $TODO_ID,
  "listID": $LIST_ID,
  "text": "Take out the trash",
  "complete": false,
  "order": 0.75
}
EOF

# Do an incremental pull from diff-server
# Note that only the second todo is returned
curl -H "Authorization:sandbox" http://localhost:7001/pull --data-binary @- << EOF
{
  "clientID":"$CLIENT_ID",
  "baseStateID":"$BASE_STATE_ID",
  "checksum":"$CHECKSUM",
  "clientViewAuth": "$USER_ID"
}
EOF

fg
```

### Step 6: 🎉🎉

That's it! You're done with the backend integration. If you haven't yet, you'll need to do the [client integration](#client-side) next.

Happy hacking!
