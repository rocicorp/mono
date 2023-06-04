---
title: The Reset Strategy
slug: /howto/diff/reset
---

# 💣 The Reset Strategy

The Reset Strategy is the easiest possible diff strategy and only really useful for the simplest, early development applications. It just sends the entire client view for every pull response.

## How it Works

### Setup

No special setup is necessary.

### On Pull

- Read the entire client view for the requesting user.
- Send a reset patch - a patch with a clear op followed by put ops for each read entity.

## Challenges

### Performance

This strategy reads the entire database the client has access to on every pull. It also _transmits_ this entire dataset to the client on every pull.

Needless to say this isn't a practical strategy for almost any real application. It's presented here only for correctness and educational reasons.

## Examples

The Get Started Guide [starts out with the Reset Strategy](/byob/client-view#serving-the-client-view) (using static data) before implementing dynamic pull.
