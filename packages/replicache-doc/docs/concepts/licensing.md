---
title: Licensing
slug: /concepts/licensing
---

The [Replicache Terms of Service](https://roci.dev/terms.html) require that anyone using
Replicache acquire and use their own license key. A license key is required for _any_ use
of Replicache, commercial or non-commercial, including tire-kicking, evaluation, and
just playing around. But don't worry: getting a key is fast, low commitment (no credit card),
and there is no charge for many uses of Replicache (see [Replicache Pricing](https://replicache.dev/#pricing)).

To get a key run:

```
npx replicache@latest get-license
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

## Monthly Active Profiles

We charge for Replicache by Monthly Active Profiles ("MAPs"). A MAP is a unique browser profile that used your application during a month.

For example, if within one month, one of your users used your Replicache-enabled app on Firefox and Chrome on their Desktop computer and Safari on their phone, that would be 3 MAPs.

The reason for counting this way is because as a client-side JavaScript library, Replicache is sandboxed within a browser profile. It can't tell the difference between two profiles on the same machine or two profiles on different machines.

MAPs are typically a small fraction (like 50%) higher than MAUs because some users, but not all, use applications on multiple profiles/devices.

## Pricing Exemption

We offer Replicache free of charge to non-commercial or pre-revenue/funding companies. See the [Terms of Service](https://rocicorp.dev/terms) for our definition of "commercial" and the [Pricing](https://replicache.dev/#pricing) page for details on the revenue/funding hurdle.

After your two-month trial of Replicache, you will receive an invoice. If you believe you qualify for one of our exemptions, you can reply to the invoice with the details.

## License Pings

We track usage by sending a ping to our servers containing your license key and a unique browser profile
identifier when Replicache is instantiated, and every 24 hours that it is running.

We check at instantiation time that your license key is valid. If your license key
is invalid, Replicache disables itself.

The license key check is asynchronous and doesn't block any other code from running. The
check is also setup so that it "fails open". The only way Replicache disables itself is
if it receives a specific message from our server. Network errors, HTTP errors, or server
errors won't cause Replicache to disable.

Disabling Replicache's pings other than via the `TEST_LICENSE_KEY` (see below) is against our [Terms of Service](https://roci.dev/terms.html). If the pings are a problem for your environment, please get in touch with us at [hello@replicache.dev](mailto:hello@replicache.dev).

## Unit testing

Replicache's license pings are almost certainly undesirable in automated
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

## Pricing Examples

- Example 1: You are a non-profit organization with 4M MAPs. **Your price is zero**.
- Example 2: You are using Replicache for a personal blog with 5k MAPs. **Your price is zero**.
- Example 3: You are a startup using Replicache for a revolutionary productivity application. You have raised a seed of $150k and have $100k annual revenue. **Your price is zero**.
- Example 4: You are using Replicache for a new version of your company's SaaS offering, but it's in internal testing and has only 50 MAPs (your dev team). You have been using Replicache for more than 2 months. Your company has raised $600k in total funding, but you are pre-revenue. **Your price is $500/mo**.
- Example 5: You are using Replicache for a new product that is a free add-on to your company's SaaS offering. You have been using Replicache for more than 2 months and are generating 15k MAPs. Your company is bootstrapped and making $300k/yr. **Your price is $3000/mo**.
