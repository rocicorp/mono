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

### Step 5: Transaction IDs

Replicache identifies mutations with a *Transaction ID*, which is a per-client incrementing integer.

Your service must track the last processed transaction ID for each client, and return it in the Client View request.

The exact mechanism to do this will vary based on how you've implemented your service, but if, for example, you are using a relational database, then a table of the following form would work:

<table>
  <tr>
    <th colspan="2">ReplicacheClientTransactions</td>
  </tr>
  <tr>
    <td>ClientID</td>
    <td>CHAR(36)</td>
  </tr>
  <tr>
    <td>LastTransactionID</td>
    <td>uint64</td>
  </tr>
 </table>

### Step 6: Upstream Sync (Server)

Replicache implements upstream sync by queuing calls to your existing server-side endpoints and invoking them later when
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

* Mutations in a particular batch must be processed **serially** to ensure proper causal consistency.
* Replicache can end up sending the same mutation multiple times. You **MUST** check whether the transaction has already been processed before doing so.
* The tracked last transactionID state for a client **MUST** be updated if a transaction has been processed, whether or not the mutation was successful. Replicache will continue to send mutations until the server acknowledges the mutation, by returning an equal or higher `lastTxID` in the Client View request.
* The tracked last transactionID state **MUST** be updated atomically with the rest of the changes caused by a mutation.
* If a request cannot be handled temporarily, return the status code "RETRY". Replicache will retry starting at the next unhandled mutation.

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
