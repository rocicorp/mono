# Offline First for Your Existing App in About an Hour

### Step 1: Create a Replicache Account

Download the `repl` CLI, then:

```
repl account create
```

This will walk you through Replicache account setup.

### Step 2: Downstream Sync

You will need to implement an *Offline View* endpoint  that returns the entire offline state for each user. Replicache will frequently query this endpoint and calculate a diff to return to each client.

The format of the offline view is just a JSON object, for example:

```
{
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
```

By default, Replicache looks for the offline view at `https://yourdomain.com/replicache-offline-view`, but you can
configure this in the client API.

### Step 3: Upstream Sync

Replicache implements upstream sync by queuing calls to your existing server-side endpoints and invoking them later when
there's connectivity in a batch. By default it posts them to `https://yourdomain.com/replicache-batch`, but you can configure this too.

The payload of the request is:

```
type BatchRequest struct {
  ClientID string
  Mutations []struct {
    // Path of query to invoke
    Path    string

    // POST payload
    Payload []byte

    // Authentation token
    Auth    string

    // Incrementing transaction number 
    TxNum   int
  }
}
```

Note that Replicache may end up calling the batch endpoint with duplicate inputs: your mutation handlers must be idempotent. You can use your existing itempotency token in the payload, or use the provided `TxNum`.

The response format is:

```
type BatchResponse struct {
  // Must match corresponding ClientID from request.
  ClientID string

  Mutations []struct {
    // Must match corresponding TxNum from request.
    TxNum   int

    // OK and FAIL are permanent. Replicache considers the request handled.
    // RETRY can only be used on the final mutation entry.
    Result  string // "OK" | "FAIL" | "RETRY"

    // Informational message. Saved to sync logs and printed to relevant client-side console.
    Message string
  }
}
```

Mutations in a particular batch must be processed **serially** to ensure proper causal consistency. If a request cannot be handled temporarily, return the status code "RETRY" or simply process fewer than the total number of provided entries. Replicache will retry starting at the next unhandled mutation.


### Step 4: Implement the Client

* [Flutter Client Setup](setup-flutter.md)
* Swift Client Setup (TODO)
* React Native Client Setup (TODO)
* Web Client Setup (TODO)
