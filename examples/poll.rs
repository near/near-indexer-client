//! Minimal stdout consumer; database consumers should commit data and checkpoint together.
use std::env;
use std::error::Error;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

use near_indexer_client::{Checkpoint, Client, Finality};
use near_kit::rpc::RpcClient;
use serde_json::json;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<_> = env::args().collect();
    if args.len() < 5 {
        return Err(
            "usage: poll URL final|optimistic CHECKPOINT_FILE BLOCK_COUNT [START_HASH]".into(),
        );
    }
    let finality = match args[2].as_str() {
        "final" => Finality::Final,
        "optimistic" => Finality::Optimistic,
        _ => return Err("finality must be final or optimistic".into()),
    };
    let path = PathBuf::from(&args[3]);
    let target: usize = args[4].parse()?;
    let client = Client::new(RpcClient::new(&args[1]), finality, 1000);
    let mut checkpoint: Option<Checkpoint> = match fs::read(&path) {
        Ok(bytes) => Some(serde_json::from_slice(&bytes)?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if checkpoint.is_none()
        && let Some(hash) = args.get(5)
    {
        let hash = hash.parse()?;
        let block = client.fetch(hash).await?;
        checkpoint = Some(Checkpoint {
            hash,
            height: block.message.block.header.height,
        });
    }
    let mut processed = 0;
    while processed < target {
        let update = client.poll(checkpoint.as_ref()).await?;
        if update.rollback.is_empty() && update.blocks.is_empty() {
            sleep(Duration::from_millis(250)).await;
            continue;
        }
        for header in &update.rollback {
            println!(
                "{}",
                json!({"event":"rollback","hash":header.hash,"height":header.height})
            );
        }
        for block in &update.blocks {
            let receipts: Vec<_> = block
                .message
                .shards
                .iter()
                .flat_map(|s| &s.receipt_execution_outcomes)
                .map(|r| {
                    json!({
                        "receipt_id": r.execution_outcome.id,
                        "receiver_id": r.receipt.receiver_id,
                        "status": r.execution_outcome.outcome.status
                    })
                })
                .collect();
            println!(
                "{}",
                json!({"event":"apply","hash":block.message.block.header.hash,"height":block.message.block.header.height,
                "prev_hash":block.message.block.header.prev_hash,"last_final_block":block.message.block.header.last_final_block,"tracked_shards":block.tracked_shards,"receipts":receipts,
                "transactions":block.message.shards.iter().filter_map(|s|s.chunk.as_ref()).map(|c|c.transactions.len()).sum::<usize>()})
            );
        }
        // Output is this demo's consumer. Flush before acknowledging. A crash
        // between stdout and checkpoint can replay; real consumers need a DB tx.
        io::stdout().flush()?;
        let mut pending = path.as_os_str().to_os_string();
        pending.push(".pending");
        let pending = PathBuf::from(pending);
        let mut file = fs::File::create(&pending)?;
        file.write_all(&serde_json::to_vec(&update.next_checkpoint)?)?;
        file.sync_all()?;
        fs::rename(pending, &path)?;
        processed += update.blocks.len();
        checkpoint = Some(update.next_checkpoint);
    }
    Ok(())
}
