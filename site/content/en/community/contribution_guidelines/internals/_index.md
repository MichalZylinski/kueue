---
title: "Internals deep dives"
linkTitle: "Internals"
weight: 40
description: >
  Chapter-by-chapter tour of the subsystems contributors touch most
type: docs
---

<!-- Written from a read of the source around the v0.19 cut. Links point at main (not a pinned commit), so files/directories stay resolvable as the code evolves; described behavior may drift in detail over time -- if something looks off, a fix or a removal is equally welcome, no need to reconcile the whole page. -->

These chapters explain how Kueue's core subsystems work, written to be read with the code open on a second screen. Read [Architecture](../learn/architecture/) first for the overall map.

The eight chapters cover: the objects and caches every change flows through (1-3), the decision engines where most scheduling bugs and features live (4-5), the framework behind the most common external contribution (6), and the two headline roadmap areas (7-8).
