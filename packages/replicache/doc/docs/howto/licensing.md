---
title: Replicache Licensing
slug: /howto/licensing
---

## Get a License Key

The [Replicache Terms of Service](https://roci.dev/terms.html) require that anyone using
Replicache acquire and use their own license key. A license key is required for _any_ use
of Replicache, commercial or non-commercial, including tire-kicking, evaluation, and
just playing around. But don't worry: getting a key is fast, low commitment (no credit card),
and there is no charge for many uses of Replicache (see [Replicache Pricing](https://replicache.dev/#pricing)).

To get a key run:

```
npx replicache@12.0.0 get-license
```

It will ask you a few questions and then print your license key, eg:

```
l123d3baa14984beca21bc42aee593064
```

Pass this key as a string to the Replicache constructor, e.g.:

```
new Replicache({
	licenseKey: "l123d3baa14984beca21bc42aee593064",
	...
});
```

## Unit testing

For reasons explained below, Replicache by default pings our server with your license key
when Replicache is instantiated. This behavior is almost certainly undesirable in automated
tests for a variety of reasons (hermeticity, inflated Replicache usage charges, etc.). For automated tests, pass
`TEST_LICENSE_KEY` instead of your key. For example:

```
import {Replicache, TEST_LICENSE_KEY} from 'replicache';
...

test('my test', () => {
	const r = new Replicache({
		licenseKey: TEST_LICENSE_KEY,
		...
	});
  ...
});
```

Using the `TEST_LICENSE_KEY` skips the server ping, but a Replicache instance
instantiated with it will shut itself down after a few minutes.

## License pings

Per [Replicache Pricing](https://replicache.dev/#pricing), we charge post-funding/revenue
commercial customers based on _Monthly Active Browser Profiles_, meaning unique browser
instances that instantiate Replicache in a calendar month. The way we accomplish this
is to send a ping to our servers containing your license key and a unique browser profile
identifier when Replicache is instantiated, and every 24 hours that it is running.
We also check at instantiation time that your license key is valid, and complain loudly
to the console if it is not. We may in the future add a feature to disable Replicache in the event that the license key is not valid.

The licensing pings explain why you want to pass `TEST_LICENSE_KEY` to Replicache in
automated tests: so that you're not potentially charged for large numbers of Replicache
instances used when running tests. (Not to mention network calls are typically undesirable
in unit tests).

Disabling Replicache's pings other than via the `TEST_LICENSE_KEY` is against our [Terms of Service](https://roci.dev/terms.html). If the pings are a problem for your environment, please get in touch with us at [hello@replicache.dev](mailto:hello@replicache.dev).
