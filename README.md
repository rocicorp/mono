# Replicache Quick Start

### Step 1: Create a Replicache Account

Download the `repl` CLI, then:

```
repl account create
```

This will walk you through Replicache account setup.

### Step 2: Downstream Sync (Server Side)

Implement a *Client View* endpoint  that returns the data that should be available locally on the client for each user. Replicache will frequently query this endpoint and calculate a diff to return to each client.

The format of the client view is JSON of the form:

```
{
  "clientID": "CB94867E-94B7-48F3-A3C1-287871E1F7FD",
  // The last Replicache transaction ID that has been processed. You will be implementing this as part of upstream sync,
  // but for now, just return 0.
  "lastTxID": 0,
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

By default, Replicache looks for the offline view at `https://yourdomain.com/replicache-offline-view`, but you can
configure this in the client API.

### Step 3: Setup the Client

See the platform-specific setup instructions:

* [Flutter Client Setup](setup-flutter.md)
* Swift Client Setup (TODO)
* React Native Client Setup (TODO)
* Web Client Setup (TODO)

### Step 4: Downstream Sync (Client Side)

Implement the client side of downstream sync:

* [Flutter Client Setup](setup-flutter.md)
* Swift Client Setup (TODO)
* React Native Client Setup (TODO)
* Web Client Setup (TODO)

### Step 4.5: Coffee Break

At this point, you have a read-only offline-first app! Reads will always be instantaneous, because they are reading from local data that is synced via Replicache and the client view.

Nice! Time for a little break. ☕️ 🍵

Next up: Writes.

### Step 5: Upstream Sync (Server)

Replicache implements upstream sync by queuing calls to your existing server-side endpoints. Queued calls are invoked when
there's connectivity in a batch. By default Replicache posts the batch to `https://yourdomain.com/replicache-batch`.

The payload of the batch request is JSON, of the form:

```
{
  "clientID": "CB94867E-94B7-48F3-A3C1-287871E1F7FD",
  "mutations": [
    {
      "txID": 7,
      "path": "/api/todo/create",
      "payload": "{\id\": \"AE2E880D-C4BD-473A-B5E0-29A4A9965EE9\", \"title\": \"Take out the trash\", ..."
    },
    {
      "txID": 8,
      "path": "/api/todo/toggle-done",
      "payload": "{\id\": \"AE2E880D-C4BD-473A-B5E0-29A4A9965EE9\", \"done\": true}"
    },
    ...
  ]
}
```

The response format is:

```json
{
  "clientID": "CB94867E-94B7-48F3-A3C1-287871E1F7FD",
  "mutations": [
    {
      "txID": 7,
      "result": "OK"
    },
    {
      "txID": 8,
      "result": "ERROR",
      "message": "Invalid POST data: syntax error: ..."
    },
    {
      "txID": 9,
      "result": "RETRY",
      "message": "Backend unavailable"
    },
  ]
}
```

Notes on correctly implementing the batch endpoint:

* Mutations in a particular batch **MUST** be processed serially in order to ensure proper [causal consistency](https://jepsen.io/consistency/models/causal).
* Replicache can end up sending the same mutation multiple times. You **MUST** ensure each transaction is processed only one time. If you have an existing idempotency token, you can send it as part of the payload of each mutation. Otherwise, you can use the `(clientID,txID)` pair as your idempotency token.
* If a request cannot be handled temporarily, return the status code `"RETRY"` and stop processing the batch. Replicache will retry starting at the next unhandled mutation. Only the last record in the response can be marked `RETRY`.

#### Batch Endpoint Psuedocode

```
let result = {
  "clientID": clientID,
  "mutations": [],
}

for mutation in mutations:
  db.beginTransaction()
  try:
    let rows = db.exec("SELECT LastTransactionID FROM ReplicacheClientTransactions WHERE ClientID=?", clientID)
    if rows.empty:
      db.exec("INSERT ReplicacheClientTransactions (ClientID, LastTransactionID) VALUES (?, ?), clientID, mutation.txID)
    else:
      if rows[0]["LastTransactionID"] >= mutation.txID:
        result.mutations.push({
        "txID": mutation.txID,
        "result": "OK",
      })
      continue
        
    db.exec("UPDATE ReplicacheClientTransactions SET LastTransactionID=? WHERE ClientID=?", mutation.txID, clientID)
    dispatchRequest(mutation.path, mutation.payload)
    db.commitTransaction()
    result.mutations.push({
      "txID": mutation.txID,
      "result": "OK",
    })
  catch (ClientError e):
    # The client has sent a bad request - consider the mutation processed and send the client an error.
    db.commitTransaction();
    result.mutations.push({
      "txID": mutation.txID,
      "result": "ERROR",
      "message": e.toString(),
    })
  catch (ServerError e):
    # Some kind of transient server error has occurred. The client should retry later.
    result.mutations.push({
      "txID": mutation.txID,
      "result": "RETRY",
      "message": e.toString(),
    })
    # no commit, the change to tracked txID state is not recorded
    # don't process any more mutations in this batch
    return result
```

### Step 7: Upstream Sync (Client)

* [Flutter - Upstream Sync](setup-flutter.md#upstream)
* Swift - Upstrem Sync (TODO)
* React Native - Upstream Sync (TODO)
* Web Client - Upstream Sync (TODO)
