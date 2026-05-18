#![cfg(feature = "replication")]

use hypercore::{Hypercore, HypercoreBuilder, PartialKeypair, Storage};
use hypercore_handshake::{
    Cipher, CipherTrait,
    state_machine::{SecStream, hc_specific::generate_keypair},
};
use std::time::Duration;
use tokio_util::compat::TokioAsyncReadCompatExt;
use uint24le_framing::Uint24LELengthPrefixedFraming;

/// Create a pair of connected in-memory encrypted streams.
fn connected_pair() -> (impl CipherTrait + 'static, impl CipherTrait + 'static) {
    let (a_b, b_a) = tokio::io::duplex(64 * 1024);
    let a_b = Uint24LELengthPrefixedFraming::new(a_b.compat());
    let b_a = Uint24LELengthPrefixedFraming::new(b_a.compat());
    let keypair = generate_keypair().unwrap();
    let initiator = Cipher::new(
        Some(Box::new(a_b)),
        SecStream::new_initiator_xx(&[]).unwrap().into(),
    );
    let responder = Cipher::new(
        Some(Box::new(b_a)),
        SecStream::new_responder_xx(&keypair, &[]).unwrap().into(),
    );
    (initiator, responder)
}

/// Create a writer (with data) and a blank reader sharing the same public key.
async fn make_writer_reader(data: &[&[u8]]) -> (Hypercore, Hypercore) {
    let mut writer = HypercoreBuilder::new(Storage::new_memory().await.unwrap())
        .build()
        .await
        .unwrap();
    for chunk in data {
        writer.append(chunk).await.unwrap();
    }
    let public_key = writer.key_pair().public;
    let reader = HypercoreBuilder::new(Storage::new_memory().await.unwrap())
        .key_pair(PartialKeypair {
            public: public_key,
            secret: None,
        })
        .build()
        .await
        .unwrap();
    (writer, reader)
}

/// Get block 0 from `reader`, with `writer`'s replicator spawned to drive the other end.
/// Uses `attach_replicator` so that `reader.get()` itself drives replication (structured
/// concurrency path).
async fn get_via_attached_replicator(writer: &Hypercore, reader: &Hypercore) -> Option<Vec<u8>> {
    let (writer_stream, reader_stream) = connected_pair();
    let writer_rep = tokio::spawn(writer.replicate(writer_stream));
    reader.attach_replicator(reader.replicate(reader_stream));
    let block = tokio::time::timeout(Duration::from_secs(5), reader.get(0))
        .await
        .expect("timed out waiting for attach_replicator get")
        .unwrap();
    writer_rep.abort();
    block
}

/// Poll until `core.info().contiguous_length >= expected`, with a 5-second timeout.
async fn wait_for_length(core: &Hypercore, expected: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut last_reported = u64::MAX;
    loop {
        let current = core.info().contiguous_length;
        if current >= expected {
            return;
        }
        if current != last_reported {
            eprintln!("contiguous_length = {current} (waiting for {expected})");
            last_reported = current;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out: contiguous_length stuck at {current}, never reached {expected}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn replicate_data_before_connect() {
    let (writer, reader) = make_writer_reader(&[b"hello", b"world"]).await;
    let (writer_stream, reader_stream) = connected_pair();

    let writer_rep = tokio::spawn(writer.replicate(writer_stream));
    let reader_rep = tokio::spawn(reader.replicate(reader_stream));

    wait_for_length(&reader, 2).await;

    assert_eq!(reader.get(0).await.unwrap(), Some(b"hello".to_vec()));
    assert_eq!(reader.get(1).await.unwrap(), Some(b"world".to_vec()));

    writer_rep.abort();
    reader_rep.abort();
}

#[tokio::test]
async fn replicate_data_after_connect() {
    let (mut writer, reader) = make_writer_reader(&[]).await;
    let (writer_stream, reader_stream) = connected_pair();

    let writer_rep = tokio::spawn(writer.replicate(writer_stream));
    let reader_rep = tokio::spawn(reader.replicate(reader_stream));

    writer.append(b"late data").await.unwrap();

    wait_for_length(&reader, 1).await;

    assert_eq!(reader.get(0).await.unwrap(), Some(b"late data".to_vec()));

    writer_rep.abort();
    reader_rep.abort();
}

/// Without an attached replicator, `get()` returns `None` immediately for a missing block.
#[tokio::test]
async fn get_returns_none_without_replicator() {
    let (_, reader) = make_writer_reader(&[b"hello"]).await;
    assert_eq!(reader.get(0).await.unwrap(), None);
}

/// `attach_replicator` makes `reader.get()` drive replication itself — no spawn needed for
/// the reader side.
#[tokio::test]
async fn attach_replicator_drives_get() {
    let (writer, reader) = make_writer_reader(&[b"hello", b"world"]).await;
    assert_eq!(
        get_via_attached_replicator(&writer, &reader).await,
        Some(b"hello".to_vec())
    );
}

/// `attach_replicator` still works when the writer appends the block *after* the connection
/// is established.
#[tokio::test]
async fn attach_replicator_drives_get_late_data() {
    let (mut writer, reader) = make_writer_reader(&[]).await;
    let (writer_stream, reader_stream) = connected_pair();
    let writer_rep = tokio::spawn(writer.replicate(writer_stream));
    reader.attach_replicator(reader.replicate(reader_stream));

    writer.append(b"late").await.unwrap();

    let block = tokio::time::timeout(Duration::from_secs(5), reader.get(0))
        .await
        .expect("timed out")
        .unwrap();
    assert_eq!(block, Some(b"late".to_vec()));
    writer_rep.abort();
}

#[tokio::test]
async fn replicate_many_blocks() {
    let data: Vec<Vec<u8>> = (0u8..10).map(|i| vec![i]).collect();
    let slices: Vec<&[u8]> = data.iter().map(|d| d.as_slice()).collect();

    let (writer, reader) = make_writer_reader(&slices).await;
    let (writer_stream, reader_stream) = connected_pair();

    let writer_rep = tokio::spawn(writer.replicate(writer_stream));
    let reader_rep = tokio::spawn(reader.replicate(reader_stream));

    wait_for_length(&reader, 10).await;

    for (i, expected) in data.iter().enumerate() {
        assert_eq!(
            reader.get(i as u64).await.unwrap().as_ref(),
            Some(expected),
            "block {i} mismatch"
        );
    }

    writer_rep.abort();
    reader_rep.abort();
}
