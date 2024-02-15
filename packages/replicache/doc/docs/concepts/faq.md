---
title: Frequently Asked Questions
slug: /concepts/faq
---

import TOCInline from '@theme/TOCInline';

<TOCInline toc={toc} />

## What is a Monthly Active Profile?

A monthly active profile (MAP) is how we charge for Replicache. Specifically it's a unique browser profile that used your application during a month.

For example, if within one month, one of your users used your Replicache-enabled app on Firefox and Chrome on their Desktop computer and Safari on their phone, that would be 3 MAPs.

The reason for counting this way is because as a client-side JavaScript library, Replicache is sandboxed within a browser profile. It can't tell the difference between two profiles on the same machine or two profiles on different machines.

MAPs are typically a small fraction (like 50%) higher than MAUs because some users, but not all, use applications on multiple profiles/devices.

## What do you mean by Commercial Application?

This is defined by the [Rocicorp Terms of Service](https://roci.dev/terms.html).

## Can you give me some billing examples?

Yes!

- Example 1: You are a non-profit organization with 4M MAPs. **Your price is zero**.
- Example 2: You are using Replicache for a personal blog with 5k MAPs. **Your price is zero**.
- Example 3: You are a startup using Replicache for a revolutionary productivity application. You have raised a seed of $150k and have $100k annual revenue. **Your price is zero**.
- Example 4: You are using Replicache for a new version of your company's SaaS offering, but it's in internal testing and has only 50 MAPs (your dev team). You have been using Replicache for more than 2 months. Your company has raised $600k in total funding, but you are pre-revenue. **Your price is $500/mo**.
- Example 5: You are using Replicache for a new product that is a free add-on to your company's SaaS offering. You have been using Replicache for more than 2 months and are generating 15k MAPs. Your company is bootstrapped and making $300k/yr. **Your price is $3000/mo**.

If you are not sure if your application is commercial or not, [drop us a line](https://replicache.dev/#contact).

## Can I get access to the source code?

Yes! We do offer source licenses to commerical users. [Let us know](https://replicache.dev/#contact) if you are interested.
