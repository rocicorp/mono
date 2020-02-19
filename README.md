# Offline First for an Existing App in 30 Minutes

## Step 1: Create a Replicache Account

Download the `repl` CLI, then:

```
repl account create
```

This will walk you through creating an account and uploading a public key.

## Step 2: Implement Your Offline View

You will need to implement an *Offline View* endpoint  that returns the entire offline state for each user.

Replicache will frequently query this endpoint and calculate a diff to return to each client.

The format of the offline view is a JSON object.

By default, Replicache looks for the offline view at `https://yourdomain.com/offline-view`, but you can
configure this in the client API.

## Step 3: Implement the Client

* [Flutter Client Setup](flutter-setup.md)
* Swift Client Setup (TODO)
* React Native Client Setup (TODO)
* Web Client Setup (TODO)

## That's It!

You're done, time for a beer 🍻!
