---
title: "Internals deep dives"
linkTitle: "Internals"
weight: 40
description: >
  Chapter-by-chapter tour of the subsystems contributors touch most
type: docs
---

<!-- Verified against main@2b494fec3 (the v0.19 cut). Re-verify each minor release. -->

These chapters explain how Kueue's core subsystems work, written to be read with the code open on a second screen. Read [Architecture](../architecture/) first for the overall map.

The eight chapters cover: the objects and caches every change flows through (1-3), the decision engines where most scheduling bugs and features live (4-5), the framework behind the most common external contribution (6), and the two headline roadmap areas (7-8).
