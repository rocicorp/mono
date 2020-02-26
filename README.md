# Replicache Quick Start

### Step 1: Create a Replicache Account

Download the `repl` CLI, then:

```
repl account create
```

This will walk you through Replicache account setup.

### Step 2: Downstream Sync (Server Side)

Implement a *Client View* endpoint  that returns the data that should be available locally on the client for each user. This endpoint should return the *entire* view (up to 20MB) every time.

Replicache will frequently query this endpoint and calculate a diff to return to each client.

The format of the client view is JSON of the form:

```jsonc
{
  "clientID": "CB94867E-94B7-48F3-A3C1-287871E1F7FD",
  // This is the last Replicache Mutation ID that you have processed.
  // You will implement this as part of Upstream Sync, but for now, just return zero.
  "lastMutationID": 0,
  "view": {
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

By default, Replicache looks for the Client View at `https://yourdomain.com/replicache-clent-view`, but you can
configure this in the client API.

### Step 3: Setup the Client

See the platform-specific setup instructions:

* [Flutter Client Setup](setup-flutter.md#setup)
* Swift Client Setup (TODO)
* React Native Client Setup (TODO)
* Web Client Setup (TODO)

### Step 4: Downstream Sync (Client Side)

Implement the client side of downstream sync:

* [Flutter Client Setup - Reads](setup-flutter.md#read-data)
* Swift Client Setup - Reads (TODO)
* React Native Client Setup - Reads (TODO)
* Web Client Setup - Reads (TODO)

### Step 4.5: Coffee Break ☕️ 🍵

At this point, you have a read-only offline-first app. Woo! Time for a little break.

Reads will always be instantaneous, because they are coming from local data that is synced via Replicache and the client view.

### Step 5: Mutation ID Storage

Next up: Writes.

Replicache identifies each change (or *Mutation*) that originates on the Replicache Client with a *MutationID*.

Your service must track the last MutationID it has processed for each client, and return it to Replicache in the Client View response. This allows Replicache to know when it can discard the speculative version of that change from the client.

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

### Step 6: Upstream Sync (Server)

Replicache implements upstream sync by queuing calls to your existing server-side endpoints. Queued calls are invoked when
there's connectivity in batches. By default Replicache posts the batch to `https://yourdomain.com/replicache-batch`.

The payload of the batch request is JSON, of the form:

```json
{
  "clientID": "CB94867E-94B7-48F3-A3C1-287871E1F7FD",
  "mutations": [
    {
      "id": 7,
      "path": "/api/todo/create",
      "payload": "{\"id\": \"AE2E880D-C4BD-473A-B5E0-29A4A9965EE9\", \"title\": \"Take out the trash\", ..."
    },
    {
      "id": 8,
      "path": "/api/todo/toggle-done",
      "payload": "{\"id\": \"AE2E880D-C4BD-473A-B5E0-29A4A9965EE9\", \"done\": true}"
    },
    ...
  ]
}
```

The response format is:

```json
{
  "mutations": [
    {
      "id": 7,
      "result": "OK"
    },
    {
      "id": 8,
      "result": "ERROR",
      "message": "Invalid POST data: syntax error: ..."
    },
    {
      "id": 9,
      "result": "RETRY",
      "message": "Backend unavailable"
    },
  ]
}
```

Notes on correctly implementing the batch endpoint:

* Replicache can end up sending the same mutation multiple times. You **MUST** ensure mutation handlers are [idempotent](https://en.wikipedia.org/wiki/Idempotence#Computer_science_meaning). We recommend that you use the provided MutationID for idempotency (see pseudocode below).
* If a request cannot be handled temporarily (e.g., because some backend component is down), return the result `"RETRY"` and stop processing the batch. Replicache will retry the remainder of the batch later.
* Once the batch endpoint returns `OK` for a mutation, that mutation **MUST** eventually be processed and reflected in the ClientView. In simple systems this will happen immediately. But it OK for processing to also be queued, as long as it does eventually happen.

#### Simple Batch Endpoint Psuedocode

```
def handleReplicacheBatch(request):
  let response = {
    mutations: [],
  };

  for mutation in request.mutations:
    let result = {
      txID: mutation.txID,
    }
    response.mutations.add(result)

    db.beginTransaction()
    let lastMutationID = getLastMutationID(request.clientID)
    if lastMutationID >= mutation.id:
      result.result = "OK"
      db.rollbackTransaction()
      continue
  
    # Handle each mutation here. Typically this will just dipatch to the existing endpoint at mutation.path.
    let err = handleMutation(mutation.path, mutation.payload)

    # For transient errors (e.g., some backend component down), stop processing the batch and tell Replicache
    # client to retry the remainder of batch later.
    if err != nil && err is TemporaryError:
      db.rollbackTransaction()
      result.result = "RETRY"
      result.message = err.Detail()
      break

    if err != nil:
      result.result = "ERROR"
      result.message = err.Detail()
    else:
      result.result = "OK"

    markMutationProcessed(request.clientID, mutation.id)
    db.commitTransaction()

  # Return the result as JSON to the Replicache client
  return result

def getLastMutationID(clientID):
  let res = db.exec("SELECT LastMutationID FROM ReplicacheMutationIDs WHERE ClientID = ?", clientID)
  if res.rows == 0:
    return NULL
  return res.rows[0].LastMutationID

def markMutationProcessed(clientID, mutationID):
  let res = db.exec("UPDATE ReplicacheMutationIDs SET LastMutationID = ? WHERE ClientID = ?", mutationID, clientID)
  if res.changedRowCount == 1:
    return
  db.exec("INSERT INTO ReplicacheMutationIDs (ClientID, LastMutationID) VALUE (?, ?)", clientID, mutationID)
```

### Step 7: Include the Last Processed Mutation ID in the Client View

In Step 2, we hardcoded `lastMutationID` to zero.

Now we're going to return the correct value so that the client can discard pending mutations that have been applied to the client view. For our simple batch endpoint pseudocode above, this would just be:

```
response.lastMutationID = getLastMutationID()
```

### Step 8: Upstream Sync (Client)

* [Flutter - Writes](setup-flutter.md#write-data)
* Swift - Writes (TODO)
* React Native - Writes (TODO)
* Web Client - Writes (TODO)
