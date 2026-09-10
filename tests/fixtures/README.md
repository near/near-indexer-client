# Fixture provenance

`block.json` and `receipts.json` were captured from a local NEAR test chain during
the HTTP indexer prototype. They contain generated test accounts, public keys,
signatures, and receipt execution data, with no private signing material.
They are distributed under this crate's MIT OR Apache-2.0 licenses.

The fixtures exercise the `EXPERIMENTAL_indexer_block` response shape, including
`tracked_shards` and explicit `proposed_split: null`. The client tests compare
normalized JSON after deserialization and serialization and synthesize ancestry
responses for replay/reorganization cases. These samples cover their recorded
protocol variants; they do not guarantee compatibility with arbitrary future
nearcore schemas.
