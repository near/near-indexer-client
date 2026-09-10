//! Public API tests with real payload fixtures and deterministic RPC responses.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use near_indexer_client::{Checkpoint, Client, CryptoHash, Error, Finality, IndexerBlock};
use near_kit::rpc::{BoxFuture, RetryConfig, RpcClient, RpcError, RpcTransport, TransportResponse};
use serde_json::{Value, json};

fn hash(id: u8) -> CryptoHash {
    CryptoHash([id; 32])
}
fn checkpoint(id: u8, height: u64) -> Checkpoint {
    Checkpoint {
        hash: hash(id),
        height,
    }
}
fn hashes(blocks: &[IndexerBlock]) -> Vec<CryptoHash> {
    blocks.iter().map(|b| b.message.block.header.hash).collect()
}

struct State {
    head: CryptoHash,
    blocks: HashMap<CryptoHash, Value>,
    unavailable: Option<CryptoHash>,
    busy: usize,
    indexer_requests: usize,
}
struct Transport(Arc<Mutex<State>>);
impl RpcTransport for Transport {
    fn post_json(
        &self,
        _url: &str,
        body: Vec<u8>,
    ) -> BoxFuture<'_, Result<TransportResponse, RpcError>> {
        let request: Value = serde_json::from_slice(&body).unwrap();
        let mut state = self.0.lock().unwrap();
        let method = request["method"].as_str().unwrap();
        let params = &request["params"];
        let result = if method == "EXPERIMENTAL_indexer_block" {
            assert_eq!(params.as_object().unwrap().len(), 1);
            let requested: CryptoHash =
                serde_json::from_value(params["block_hash"].clone()).unwrap();
            state.indexer_requests += 1;
            if state.busy > 0 {
                state.busy -= 1;
                Err("BUSY")
            } else if state.unavailable == Some(requested) {
                Err("DATA_UNAVAILABLE")
            } else {
                state
                    .blocks
                    .get(&requested)
                    .cloned()
                    .ok_or("DATA_UNAVAILABLE")
            }
        } else {
            assert_eq!(method, "block");
            let requested = if params.get("finality").is_some() {
                assert!(matches!(
                    params["finality"].as_str(),
                    Some("final" | "optimistic")
                ));
                state.head
            } else {
                serde_json::from_value(params["block_id"].clone()).unwrap()
            };
            state
                .blocks
                .get(&requested)
                .map(|b| json!({"header":b["block"]["header"]}))
                .ok_or("UNKNOWN_BLOCK")
        };
        let response = match result {
            Ok(result) => json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
            Err(name) => {
                json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"server error","data":{"name":name}}})
            }
        };
        Box::pin(async move {
            Ok(TransportResponse {
                status: 200,
                body: serde_json::to_vec(&response).unwrap(),
            })
        })
    }
}
fn setup(finality: Finality, limit: usize, retries: u32) -> (Client, Arc<Mutex<State>>) {
    let mut blocks = HashMap::new();
    for (id, parent, height) in [
        (1, 0, 1),
        (2, 1, 3),
        (3, 2, 4),
        (4, 1, 3),
        (5, 4, 4),
        (6, 5, 6),
    ] {
        let mut value: Value = serde_json::from_str(include_str!("fixtures/block.json")).unwrap();
        value["block"]["header"]["hash"] = json!(hash(id));
        value["block"]["header"]["prev_hash"] = json!(hash(parent));
        value["block"]["header"]["height"] = json!(height);
        value["block"]["header"]["last_final_block"] = json!(hash(0));
        blocks.insert(hash(id), value);
    }
    let state = Arc::new(Mutex::new(State {
        head: hash(3),
        blocks,
        unavailable: None,
        busy: 0,
        indexer_requests: 0,
    }));
    let rpc = RpcClient::with_transport_and_retry_config(
        "http://fixture",
        Arc::new(Transport(state.clone())),
        RetryConfig {
            max_retries: retries,
            initial_delay_ms: 0,
            max_delay_ms: 0,
        },
    );
    (Client::new(rpc, finality, limit), state)
}

#[test]
fn published_schema_preserves_real_block_and_receipt_payloads() {
    for fixture in [
        include_str!("fixtures/block.json"),
        include_str!("fixtures/receipts.json"),
    ] {
        let original: Value = serde_json::from_str(fixture).unwrap();
        let decoded: IndexerBlock = serde_json::from_value(original.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), original);
    }
    let receipts: IndexerBlock =
        serde_json::from_str(include_str!("fixtures/receipts.json")).unwrap();
    assert!(
        receipts
            .message
            .shards
            .iter()
            .any(|s| !s.receipt_execution_outcomes.is_empty())
    );
}

#[test]
fn split_null_and_missing_remain_distinct() {
    let mut value: Value = serde_json::from_str(include_str!("fixtures/block.json")).unwrap();
    value["block"]["chunks"][0]
        .as_object_mut()
        .unwrap()
        .remove("proposed_split");
    value["shards"][0]["chunk"]["header"]
        .as_object_mut()
        .unwrap()
        .remove("proposed_split");
    let decoded: IndexerBlock = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(decoded.message.block.chunks[0].proposed_split, None);
    assert_eq!(decoded.message.block.chunks[1].proposed_split, Some(None));
    assert_eq!(
        decoded.message.shards[0]
            .chunk
            .as_ref()
            .unwrap()
            .header
            .proposed_split,
        None
    );
    assert_eq!(
        decoded.message.shards[1]
            .chunk
            .as_ref()
            .unwrap()
            .header
            .proposed_split,
        Some(None)
    );
    assert_eq!(serde_json::to_value(decoded).unwrap(), value);
}

#[tokio::test]
async fn explicit_target_stays_pinned_and_does_not_claim_finality() {
    let (client, state) = setup(Finality::Final, 10, 0);
    state.lock().unwrap().head = hash(6);
    let update = client
        .update_to(Some(&checkpoint(1, 1)), hash(3))
        .await
        .unwrap();
    assert_eq!(hashes(&update.blocks), [hash(2), hash(3)]);
    assert_eq!(update.next_checkpoint, checkpoint(3, 4));
    assert_eq!(update.finality, Finality::Optimistic);
}

#[tokio::test]
async fn explicit_target_rejects_wrong_header_and_excessive_ancestry() {
    let (client, state) = setup(Finality::Optimistic, 1, 0);
    assert!(matches!(
        client.update_to(Some(&checkpoint(1, 1)), hash(3)).await,
        Err(Error::AncestryLimit(1))
    ));
    state.lock().unwrap().blocks.get_mut(&hash(3)).unwrap()["block"]["header"]["hash"] =
        json!(hash(5));
    assert!(matches!(
        client.update_to(None, hash(3)).await,
        Err(Error::InvalidData(_))
    ));
}

#[tokio::test]
async fn catches_up_by_hash_across_skipped_heights() {
    let (client, _) = setup(Finality::Final, 10, 0);
    let update = client.poll(Some(&checkpoint(1, 1))).await.unwrap();
    assert!(update.rollback.is_empty());
    assert_eq!(hashes(&update.blocks), [hash(2), hash(3)]);
    assert_eq!(update.next_checkpoint, checkpoint(3, 4));
    assert_eq!(update.finality, Finality::Final);
    assert_eq!(update.head.last_final_block, hash(0));
}

#[tokio::test]
async fn same_height_fork_replays_after_unacknowledged_restart() {
    let (client, state) = setup(Finality::Optimistic, 10, 0);
    state.lock().unwrap().head = hash(5);
    let saved = serde_json::to_vec(&checkpoint(3, 4)).unwrap();
    for _ in 0..2 {
        let restored = serde_json::from_slice(&saved).unwrap();
        let update = client.poll(Some(&restored)).await.unwrap();
        assert_eq!(
            update.rollback.iter().map(|h| h.hash).collect::<Vec<_>>(),
            [hash(3), hash(2)]
        );
        assert_eq!(hashes(&update.blocks), [hash(4), hash(5)]);
    }
    state.lock().unwrap().head = hash(6);
    let update = client.poll(Some(&checkpoint(5, 4))).await.unwrap();
    assert!(update.rollback.is_empty());
    assert_eq!(hashes(&update.blocks), [hash(6)]);
}

#[tokio::test]
async fn unchanged_head_and_initial_head() {
    let (client, state) = setup(Finality::Final, 10, 0);
    let empty = client.poll(Some(&checkpoint(3, 4))).await.unwrap();
    assert!(empty.rollback.is_empty() && empty.blocks.is_empty());
    assert_eq!(state.lock().unwrap().indexer_requests, 0);
    assert_eq!(hashes(&client.poll(None).await.unwrap().blocks), [hash(3)]);
}

#[tokio::test]
async fn finalized_mode_rejects_branch_replacement_before_fetching_payloads() {
    let (client, state) = setup(Finality::Final, 10, 0);
    state.lock().unwrap().head = hash(5);
    assert!(matches!(
        client.poll(Some(&checkpoint(3, 4))).await,
        Err(Error::FinalityViolation)
    ));
    assert_eq!(state.lock().unwrap().indexer_requests, 0);
}

#[tokio::test]
async fn exhausted_missing_data_returns_no_partial_update() {
    let (client, state) = setup(Finality::Final, 10, 1);
    state.lock().unwrap().unavailable = Some(hash(3));
    let checkpoint = checkpoint(1, 1);
    assert!(matches!(
        client.poll(Some(&checkpoint)).await,
        Err(Error::Rpc(_))
    ));
    assert_eq!(checkpoint.hash, hash(1));
    // Block 2 was fetched, but no update/checkpoint was returned. Replay it.
    state.lock().unwrap().unavailable = None;
    assert_eq!(
        hashes(&client.poll(Some(&checkpoint)).await.unwrap().blocks),
        [hash(2), hash(3)]
    );
}

#[tokio::test]
async fn transport_policy_retries_busy_then_exhausts_without_advancing() {
    let (client, state) = setup(Finality::Final, 10, 2);
    state.lock().unwrap().busy = 2;
    assert_eq!(
        client
            .fetch(hash(3))
            .await
            .unwrap()
            .message
            .block
            .header
            .hash,
        hash(3)
    );
    assert_eq!(state.lock().unwrap().indexer_requests, 3);
    state.lock().unwrap().busy = 4;
    assert!(matches!(
        client.poll(Some(&checkpoint(1, 1))).await,
        Err(Error::Rpc(_))
    ));
    assert_eq!(state.lock().unwrap().indexer_requests, 6);
}

#[tokio::test]
async fn incomplete_coverage_and_hash_mismatch_fail_closed() {
    let (client, state) = setup(Finality::Final, 10, 0);
    let original = state.lock().unwrap().blocks[&hash(3)].clone();
    for mutation in 0..4 {
        let mut block = original.clone();
        match mutation {
            0 => block["tracked_shards"] = json!([]),
            1 => {
                block["shards"].as_array_mut().unwrap().pop();
            }
            2 => {
                let shard = block["shards"][0].clone();
                block["shards"].as_array_mut().unwrap().push(shard);
            }
            _ => block["block"]["header"]["hash"] = json!(hash(6)),
        }
        state.lock().unwrap().blocks.insert(hash(3), block);
        assert!(matches!(
            client.fetch(hash(3)).await,
            Err(Error::InvalidData(_))
        ));
    }
}

#[tokio::test]
async fn bounded_or_missing_ancestry_and_bad_checkpoint_fail_closed() {
    let (client, state) = setup(Finality::Optimistic, 2, 0);
    state.lock().unwrap().head = hash(6);
    assert!(matches!(
        client.poll(Some(&checkpoint(3, 4))).await,
        Err(Error::AncestryLimit(2))
    ));
    assert!(matches!(
        client.poll(Some(&checkpoint(3, 99))).await,
        Err(Error::InvalidData(_))
    ));
    state.lock().unwrap().blocks.remove(&hash(3));
    assert!(matches!(
        client.poll(Some(&checkpoint(3, 4))).await,
        Err(Error::Rpc(_))
    ));
    let (zero, _) = setup(Finality::Final, 0, 0);
    assert!(matches!(
        zero.poll(None).await,
        Err(Error::AncestryLimit(0))
    ));
}
