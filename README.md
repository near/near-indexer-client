# near-indexer-client

Follow the NEAR chain from your own code, without running a node.

`near-indexer-client` reads complete blocks from a NEAR RPC node's
`EXPERIMENTAL_indexer_block` endpoint: every transaction, receipt, execution
outcome, and state change, as the standard `StreamerMessage`. You keep a
checkpoint; the client tells you what to apply next.

```sh
cargo add near-indexer-client
```

## Quick start

```rust
use near_indexer_client::near_kit::rpc::RpcClient;
use near_indexer_client::{Checkpoint, Client, Finality};

async fn follow(mut checkpoint: Option<Checkpoint>) -> Result<(), near_indexer_client::Error> {
    let client = Client::new(RpcClient::new("http://127.0.0.1:3030"), Finality::Final, 1000);
    loop {
        let update = client.poll(checkpoint.as_ref()).await?;
        for block in &update.blocks {
            println!("{}", block.message.block.header.height);
        }
        // Save your results and `update.next_checkpoint` in one transaction.
        checkpoint = Some(update.next_checkpoint);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
```

Start with `None` to begin at the current head. After that, always pass the last
checkpoint you saved.

## How it works

- **You own the checkpoint.** The client keeps no cursor, queue, database, or
  background task. A checkpoint is exclusive: its block counts as already
  processed. If you crash before saving, the next poll replays the same blocks.
- **Updates are all or nothing.** `poll` either returns every block you need,
  or an error with your checkpoint untouched. It never returns a partial or
  silently empty block.
- **Final or optimistic.** `Finality::Final` follows finalized blocks and never
  rolls back. `Finality::Optimistic` follows the latest head. When the head
  changes branch, the update lists `rollback` headers (newest first) to undo
  before applying `blocks` (oldest first), including same-height replacements.
- **Long catch-ups stay bounded.** `poll` holds a whole update in memory. If
  you're far behind, call `head()` and `plan(checkpoint, head)` to get headers
  only, then `fetch_block` each one and save a checkpoint every few blocks. A
  failed request repeats only that block.
- **Gaps are fine.** Heights can skip; the client follows parent hashes, not
  heights, and a shard with no new chunk in a block has `chunk: None`.
  `max_ancestry` (the last argument to `Client::new`) caps how far one
  update may walk, and exceeding it is an error rather than a truncated batch.

## Errors and retries

The client never sleeps or retries on its own. You decide when to try again.

- `Error::kind()` names the failure, using the node's error name where it has
  one (`BUSY`, `DATA_UNAVAILABLE`, `LIMIT_EXCEEDED`, ...).
- `RetryPolicy` and `Backoff` pace retries per request. A busy, timed-out, or
  unreachable node backs off from 500 ms to 10 s. Missing data stays retryable
  for 30 s, since a node can announce a head before it saves the data.
  Everything else backs off up to 60 s.
- If you use `Backoff`, build the `RpcClient` with `RetryConfig::none()` so
  near-kit's own retries don't stack underneath.
- An error tied to one block, such as a response over the node's size limit,
  hits every consumer of that block. It needs a fix on the node, not a retry.

## Running a serving node

Public RPC endpoints don't offer this method. You need a node you control,
running nearcore 2.14.0 or later, with:

```json
{
  "rpc": { "enable_indexer_rpc": true },
  "tracked_shards_config": "AllShards",
  "save_tx_outcomes": true,
  "save_state_changes": true,
  "store": { "save_trie_changes": true }
}
```

The node can only serve history it kept with these settings on, and archive
mode alone isn't enough. By default it serves one indexer request at a time
and rejects the rest with `BUSY`; raise `rpc.indexer_max_concurrent_requests`
if several consumers share it. The method isn't available under Spice
execution.

The client trusts the node's view of the chain. It checks that every shard is
present and that blocks link up, but it doesn't verify consensus or proofs.

## Example

```sh
cargo run --example poll -- http://127.0.0.1:3030 final /tmp/checkpoint.json 10
```

The arguments are `URL final|optimistic CHECKPOINT_FILE BLOCK_COUNT [START_HASH]`.
It prints one JSON line per block and saves its checkpoint to a file after each
update. A real consumer should save its data and checkpoint in the same
database transaction instead.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development checks and
[RELEASING.md](RELEASING.md) for the release process.

Licensed under MIT or Apache-2.0.
