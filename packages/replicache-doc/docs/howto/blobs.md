---
title: Work with Blobs
slug: /howto/blobs
---

Binary data is often referred to as "blobs". This recipe shows a few ways to use
binary data in Replicache.

The data model in Replicache is [JSON](https://www.json.org/json-en.html). JSON
does not have a way to represent binary data efficiently. Depending on your use
case there are a few ways to handle binary data.

## Base64

The simplest way to handle binary data is to
[base64](https://en.wikipedia.org/wiki/Base64) encode it. The size overhead for
base64 is roughly 4/3 (or 133%) of the size of the original data. You can reduce
this by using a compression algorithm but that adds more complexity to the code.

For example if you have a profile picture in your user data you can do something like:

```ts
// npm install base64-arraybuffer
import * as base64 from 'base64-arraybuffer';

type User = {
  id: string;
  name: string;
  picture: Uint8Array;
};

type RepUser = {
  id: string;
  name: string;
  // Note how Replicache needs to use a string here!
  picture: string;
};

const rep = new Replicache({
  name: 'user-id',
  mutators: {
    async setUserData(tx: WriteTransaction, user: RepUser) {
      await tx.set(`user/${user.id}`, user);
    },
  },
});

async function setUserData(rep: Replicache, user: User) {
  const {id, name, picture} = user;
  const repUser = {
    id,
    name,
    picture: base64.encode(picture.buffer),
  };
  await rep.mutate.setUserData(repUser);
}

async function getUserData(rep: Replicache, id: string): Promise<User> {
  const repUser = await rep.query(tx => tx.get(`user/${id}`));
  const {id, name, picture} = repUser;
  return {
    id,
    name,
    picture: new Uint8Array(base64.decode(picture)),
  };
}
```

### Best practices when using base64

If your binary data is not small and does not change frequently it is probably
better to keep it in its own key. This way we do not have to redownload the data
when some unrelated data changes.

If we continue with the above example, we can store the picture in its own key
by doing something like.

```ts
const rep = new Replicache({
  name: 'user-id',
  mutators: {
    async setUserData(tx: WriteTransaction, user: RepUser) {
      const {id, name, picture} = user;
      await tx.set(`user/${id}`, {id, name});
      await tx.set(`user/${id}/picture`, picture);
    },
  },
});

async function getUserData(rep: Replicache, id: string): Promise<User> {
  const {name, picture} = await rep.query(async tx => {
    const {name} = await tx.get(`user/${id}`);
    const picture = await tx.get(`user/${id}/picture`);
    return {name, picture};
  });
  return {
    id,
    name,
    picture: new Uint8Array(base64.decode(picture)),
  };
}
```

Now, if the name changes we do not need to resync the picture data.

## Content Addressed Data

If the data is immutable and large and is often shared between different parts
of the system it might make sense to use content addressed data. When using
content adressed data we compute a hash of the content and use that as the key.

Modern browsers have excellent support for hashing so it is easy to have the
client compute the hash.

If we continue with the above example, we can use the hash of the picture as its ID.

```ts
type RepUser = {
  id: string;
  name: string;
  picture: string;
  pictureHash: string;
};

async function computeHash(data: Uint8Array): Promise<string> {
  const buf = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(buf), b =>
    b.toString(16).padStart(2, '0'),
  ).join('');
}

const rep = new Replicache({
  name: 'user-id',
  mutators: {
    async setUserData(tx: WriteTransaction, user: RepUser) {
      const {id, name, picture, pictureHash} = user;
      await tx.set(`user/${id}`, {id, name, pictureHash});
      await tx.set(`blob/${pictureHash}`, picture);
    },
  },
});

async function setUserData(rep: Replicache, user: User) {
  const {id, name, picture} = user;
  const pictureHash = await computeHash(picture);
  const repUser = {
    id,
    name,
    picture: base64.encode(picture.buffer),
    pictureHash,
  };
  await rep.mutate.setUserData(repUser);
}

async function getUserData(rep: Replicache, id: string): Promise<User> {
  const {name, picture} = await rep.query(async tx => {
    const {name, pictureHash} = await tx.get(`user/${id}`);
    const picture = await tx.get(`blob/${pictureHash}`);
    return {name, picture};
  });
  return {
    id,
    name,
    picture: new Uint8Array(base64.decode(picture)),
  };
}
```

## Storing binary data outside of Replicache

It is also possible to store binary data outside of Replicache.

This gets significantly more complicated and it is important to point out that
since the data is no longer managed by Replicache there is no guarantee that the
blobs stays consistend with the state of Replicache. User code needs to handle
the case where a referenced blob isn't downloaded yet as well as manage the
syncing of the blobs.

The main reason to store binary data outside the client view (outside
Replicache) is to exceed the size limits of the client view of Replicache itself
as well as the size limit imposed by "serverless" servers. For example [AWS
Lambda](https://aws.amazon.com/lambda/) limits the size of response/requst to
[6MB](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html).
When using things like [Amazon S3](https://aws.amazon.com/s3/) for the actual
storage you can upload and download directly to the S3 bucket which allows you
to sidestep the request size limit of you server functions.

Another benifit of this approach is that we do not need to read large blobs into
memory and we can let the browser keep things on disk as needed. This might be
important if your app is working with large media files for example.

To make things a little bit simpler we are going to treat blobs as immutable and
use content adressed data.

We are going to walk through an example where we store the blobs locally in a
[CacheStorage](https://developer.mozilla.org/en-US/docs/Web/API/CacheStorage).
We will continue with the profile picture as an example.

### Setting up the server

Since we need to sync the data between clients and the data is no longer managed
by Replicache we need the server to cooperate. The server will need an endpoint
to upload the data to and another endpoint to download the data from.

#### Upload endpoint

The upload endpoint will be a `PUT` handler at `/blob/<hash>`. The hash is the content address of the blob.

#### Download endpoint

This endpoint will be a `GET` handler at `/blob/<hash>`. The hash is the content address of the blob.

### Keep the blob hashes in the Client View

To sync the blobs to the client we keep the hashes of the blobs in the client
view. We subscribe to changes of a keyspace of the client view and whenever this
changes we download the files as needed.

```ts
const blobPrefx = 'blob/';

rep.subscribe(
  (tx: ReadTransaction) => tx.scan({prefix: blobPrefix}).keys().toArray(),
  {
    async onData(keys: string[]) {
      for (const key of keys) {
        const hash = key.slice(blobPrefix.length);
        await downloadBlob(hash);
      }
    },
  },
);

// This should be the same as the name used with Replicache.
const cacheName = 'profile-pictures';

const blobURL = hash => `/blob/${hash}`;
const blobKey = hash => `blob/${hash}`;

async function downloadBlob(hash: string) {
  // Check if we already have the blob.
  const cache = await caches.open(cacheName);
  const url = blobURL(hash);
  const resp = await cache.match(url);
  if (!resp) {
    // not in cache
    const r = await fetch(url);
    if (r.ok) {
      await cache.put(url, r);
    } else {
      // Try again next time.
      // TODO: handle error better
    }
  }
}
```

#### Uploading the blob

We could just upload the blob and sync the data using a `pull`, which would in
turn download the file. This is the simplest way to do it but the downside is
that we have to redownload the file directly after we upload it. This is going
to be slow, especially for large media files. One way to prevent this is to add
the file to the cache and keep the uploaded state in the client view as well.

```ts
async function uploadBlob(rep: Replicache, data: Uint8Array, hash: string) {
  // Since we already have the blob here, we might as well add it to
  // the cache instead of redownloading it.
  await addBlobToCache(hash, data);
  const resp = await fetch(blobURL(hash), {
    method: 'PUT',
    body: data,
  });
  await rep.mutate.addBlob({hash, shouldUpload: !resp.ok});
}

async function addBlobToCache(hash: string, data: Uint8Array) {
  const cache = await caches.open(cacheName);
  const blob = new Blob([data]);
  await cache.put(blobURL(hash), new Response(blob));
}

const rep = new Replicache({
  name: 'user-id',
  mutators: {
    async addBlob(tx, {hash, shouldUpload}) {
      await tx.set(blobKey(hash), {shouldUpload});
    },
  },
});
```

The server side implementation of the `addBlob` mutator does not have to do
much. It can just store the key and value as pushed. It is important that the
client view includes the key-value pair for this blob or the client will lose
track of the state of the blob. A better implementation would be to set
`shouldUpload` depending on wether the blob has been uploaded or not.

#### Syncing the blobs

We didn't do a very good job dealing with errors above. Let's change the
subscription to deal with both upload and download now that we are keeping track
of the uploaded state.

```ts
rep.subscribe(tx => tx.scan({prefix: blobPrefix}).entries().toArray(), {
  async onData(blobs: [string, {shouldUpload: boolean}][]) {
    const cache = await caches.open(cacheName);
    for (const [key, value] of blobs) {
      const hash = key.slice(blobPrefix.length);
      const {shouldUpload} = value;
      await syncBlob(rep, cache, hash, shouldUpload);
    }
  },
});

async function syncBlob(rep, cache, hash, shouldUpload) {
  const response = await cache.match(blobURL(hash));
  if (response) {
    if (shouldUpload) {
      const buffer = await response.arrayBuffer();
      await uploadBlob(rep, new Uint8Array(buffer), hash);
    }
  } else {
    const resp = await downloadBlob(hash);
    if (resp.ok) {
      await cache.put(blobURL(hash), resp);
      if (shouldUpload) {
        // Mark as uploaded, so we don't try to upload it again.
        await rep.mutate.addBlob({hash, shouldUpload: false});
      }
    }
  }
}
```

Change download blob to do nothing but download...

```ts
function downloadBlob(hash) {
  return fetch(blobURL(hash));
}
```

The above code should now work for both upload and download. When we add a blob
we register the hash in Replicache and we store the blob in a CacheStorage
cache. We subscribe to changes in Replicache keys starting with `'blob/'` and
resync the file as needed when this changes.

#### Failure to upload or download

In any network app the network can go down. In case we failed to upload or
download the files we need to handle the cases where upload or download
failed and keep trying. We can run the sync code on an interval to keep trying.

We can extract some of the above code and put it in an `setInterval`.

```ts
const blobsTx = (tx: ReadTransaction) =>
  tx.scan({prefix: blobPrefix}).entries().toArray();

async function syncAllBlobs(blobs: [string, {shouldUpload: boolean}][]) {
  const cache = await caches.open(cacheName);
  for (const [key, value] of blobs) {
    const hash = key.slice(blobPrefix.length);
    const {shouldUpload} = value;
    await syncBlob(rep, cache, hash, shouldUpload);
  }
}

rep.subscribe(blobsTx, {
  onData: syncAllBlobs,
});

setInterval(
  async () => {
    const blobs = await rep.query(blobsTx);
    await syncAllBlobs(blobs);
  },
  5 * 60 * 1000,
);
```

This will run the sync code every 5 minutes. This is of course a bit too
simplistic. We should ensure that there are no overlapping syncs and we can keep
track of network failure to detect if we need to retry or not.

#### Pull Response

The above works well for blobs added by the current client. However, if we want
to get blobs from other clients we need to ensure that the pull response
includes the hashes of the blobs from them too.

In this simple case we can check if a key starting with `user/` is included in
the pull response and if so also add an op to set the blob key in that case as
well. In a more mature system you probably want to design a more solid solution.
