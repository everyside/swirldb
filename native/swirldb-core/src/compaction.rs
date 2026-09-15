// Copyright 2026 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Compaction: a document's history folded into one change that writes its
//! current state.
//!
//! **Why.** An Automerge document keeps every change anybody ever made, and
//! nothing else in SwirlDB ever lets one go. Every open sends the whole
//! history to the client, and every load materializes every operation in it,
//! so a document costs what its past weighs rather than what it holds. On
//! 2026-09-12 a Studio pattern whose source is 1,584 characters carried 1,085
//! changes and 360,953 operations, and loading it is how its sync server was
//! OOMKilled ten times.
//!
//! **What Automerge allows.** A change names the hashes of the changes it
//! depends on, all the way back to the first, and a document cannot hold a
//! change whose dependencies it lacks: it queues it and waits. So there is no
//! keeping "the recent history" and dropping the old: the recent changes
//! depend on the old ones. The only fold there is replaces the whole graph
//! with a new one, a single change with no dependencies that puts the current
//! value of everything. That new document has the same state and none of the
//! old hashes.
//!
//! **What that means for a peer.** A peer that still holds the old history
//! shares no change with the snapshot. Merging the two would not be an error —
//! it would be worse: every key would conflict between the old object and the
//! snapshot's copy of it, which one wins would be decided by operation
//! counters rather than by which is newer, and everything the peer wrote
//! afterwards would depend on history the server no longer has and be queued
//! there for ever. The test
//! `a_peer_holding_the_history_from_before_a_compaction_cannot_sync_with_it`
//! below walks through each step.
//!
//! So a document is only compacted when nobody has it open (the server's
//! rule, in `swirldb-server`'s state); a client that opens with heads from
//! before a compaction is told `compacted` rather than handed a merge; and a
//! push whose changes depend on missing history is refused rather than
//! acknowledged. `Connection.openDocument` in the browser and `SyncClient` in
//! Rust both open a document from empty, so neither ever presents old heads;
//! the refusals are for a handle that kept its own copy — the browser's
//! `withIndexedDB` followed by `connect` — and for a race nobody has found.
//!
//! **What is kept, and what is not.** Every map, list and text with the value
//! it shows now; scalars with their types, counters as counters. Not kept:
//! the losing side of a conflict (a key two people put at once keeps the
//! value everyone already reads), marks on a text (nothing in SwirlDB writes
//! them), and of course the history itself — who changed what, and when.

use anyhow::{anyhow, Result};
use automerge::{
    transaction::{CommitOptions, Transactable},
    AutoCommit, ObjId, ObjType, ReadDoc, ScalarValue, TextEncoding, Value, ROOT,
};

/// The message on a compaction's one change, which is how a document says it
/// was compacted: see [`is_compacted`].
pub const COMPACTION_MESSAGE: &str = "swirldb: compacted";

/// How much history a document carries: its changes, and the bytes those
/// changes weigh as Automerge encodes each one — which is also what a full
/// sync sends over the wire when somebody opens the document.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct History {
    pub changes: usize,
    pub bytes: usize,
    /// The bytes of the compaction the history begins with, if it begins
    /// with one: the weight of the document's state as of that compaction,
    /// which is part of `bytes` but is not history anybody wrote since.
    pub snapshot_bytes: usize,
}

impl History {
    /// The bytes written since the last compaction, or ever.
    pub fn bytes_since_compaction(&self) -> usize {
        self.bytes - self.snapshot_bytes
    }
}

/// When a document's history is folded: once it passes either bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionThreshold {
    /// Changes in the history.
    pub changes: usize,
    /// Bytes of changes written since the last compaction, as
    /// [`History::bytes_since_compaction`] counts them.
    pub bytes: usize,
}

impl CompactionThreshold {
    /// Five hundred changes or a hundred kilobytes of them, whichever comes
    /// first. Chosen from Studio's staging data on 2026-09-15: of 181
    /// documents, 173 sat under both bounds, and the eight over one of them
    /// were the ones costing memory — up to 7,574 changes, 1.2 MB of history
    /// and 360,953 operations. The bytes are counted from the last compaction
    /// on, because a state can itself weigh more than the bound (the heaviest
    /// of those eight weighs 369 KB as one change) and a document that
    /// counted its own snapshot would be compacted again on every unload.
    pub const DEFAULT: CompactionThreshold = CompactionThreshold {
        changes: 500,
        bytes: 100_000,
    };

    /// Whether a document with this history should be compacted. A history of
    /// one change is never due: it is already as short as a history can be,
    /// and a compaction of it would only mint new hashes for the same state.
    pub fn is_due(&self, history: History) -> bool {
        history.changes > 1
            && (history.changes >= self.changes || history.bytes_since_compaction() >= self.bytes)
    }
}

impl Default for CompactionThreshold {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// What a compaction did, for a log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Compaction {
    pub before: History,
    pub after: History,
}

/// The history of a document.
pub fn history(document: &mut AutoCommit) -> History {
    let changes = document.get_changes(&[]);
    History {
        changes: changes.len(),
        bytes: changes.iter().map(|change| change.raw_bytes().len()).sum(),
        snapshot_bytes: changes
            .first()
            .filter(|change| is_compaction(change))
            .map_or(0, |change| change.raw_bytes().len()),
    }
}

/// Whether a document's history begins with a compaction.
pub fn is_compacted(document: &mut AutoCommit) -> bool {
    document
        .get_changes(&[])
        .first()
        .is_some_and(|change| is_compaction(change))
}

/// A compaction's change: no dependencies, and [`COMPACTION_MESSAGE`].
fn is_compaction(change: &automerge::Change) -> bool {
    change.deps().is_empty() && change.message().map(String::as_str) == Some(COMPACTION_MESSAGE)
}

/// A new document holding `document`'s current state as one change and no
/// other history. Text positions in the new document are counted in
/// `text_encoding`, which changes nothing stored: see
/// [`crate::SwirlDB::new_with_text_encoding`].
pub fn snapshot(document: &AutoCommit, text_encoding: TextEncoding) -> Result<AutoCommit> {
    let mut snapshot = AutoCommit::new_with_encoding(text_encoding);
    copy_map(document, &ROOT, &mut snapshot, &ROOT)?;
    // The change is empty only when the document is; an empty document's
    // snapshot is an empty document, with no change at all.
    snapshot.commit_with(CommitOptions::default().with_message(COMPACTION_MESSAGE));
    Ok(snapshot)
}

fn copy_map(
    from: &AutoCommit,
    from_object: &ObjId,
    to: &mut AutoCommit,
    to_object: &ObjId,
) -> Result<()> {
    for item in from.map_range(from_object, ..) {
        let key = item.key.to_string();
        match item.value {
            Value::Object(object_type) => {
                let created = to
                    .put_object(to_object, key.as_str(), object_type)
                    .map_err(|e| anyhow!("Failed to copy {}: {:?}", key, e))?;
                copy_object(from, &item.id, object_type, to, &created)?;
            }
            Value::Scalar(scalar) => {
                to.put(to_object, key.as_str(), copied_scalar(&scalar))
                    .map_err(|e| anyhow!("Failed to copy {}: {:?}", key, e))?;
            }
        }
    }
    Ok(())
}

fn copy_list(
    from: &AutoCommit,
    from_object: &ObjId,
    to: &mut AutoCommit,
    to_object: &ObjId,
) -> Result<()> {
    for (index, item) in from.list_range(from_object, ..).enumerate() {
        match item.value {
            Value::Object(object_type) => {
                let created = to
                    .insert_object(to_object, index, object_type)
                    .map_err(|e| anyhow!("Failed to copy item {}: {:?}", index, e))?;
                copy_object(from, &item.id, object_type, to, &created)?;
            }
            Value::Scalar(scalar) => {
                to.insert(to_object, index, copied_scalar(&scalar))
                    .map_err(|e| anyhow!("Failed to copy item {}: {:?}", index, e))?;
            }
        }
    }
    Ok(())
}

fn copy_object(
    from: &AutoCommit,
    from_object: &ObjId,
    object_type: ObjType,
    to: &mut AutoCommit,
    to_object: &ObjId,
) -> Result<()> {
    match object_type {
        ObjType::Map | ObjType::Table => copy_map(from, from_object, to, to_object),
        ObjType::List => copy_list(from, from_object, to, to_object),
        ObjType::Text => {
            let text = from
                .text(from_object)
                .map_err(|e| anyhow!("Failed to read a text: {:?}", e))?;
            to.splice_text(to_object, 0, 0, &text)
                .map_err(|e| anyhow!("Failed to copy a text: {:?}", e))
        }
    }
}

/// A scalar as the snapshot writes it. A counter is written as a counter
/// holding its current total, so incrementing it afterwards still merges as
/// an increment; everything else is written as it reads.
fn copied_scalar(scalar: &ScalarValue) -> ScalarValue {
    match scalar {
        ScalarValue::Counter(counter) => ScalarValue::counter(i64::from(counter)),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SwirlDB;
    use serde_json::json;

    /// A document the way Studio writes one: a text typed into splice by
    /// splice, a list of maps edited in place, a scalar replaced many times —
    /// each write its own change, as a client pushing every write makes it.
    fn a_worked_document() -> SwirlDB {
        let db = SwirlDB::new();
        let write = |result: Result<()>| {
            result.unwrap();
            // Reading the heads commits what was written as one change.
            db.get_heads();
        };
        write(db.set_text("source", ""));
        for (index, character) in "export fn render() {}".chars().enumerate() {
            write(db.splice_text("source", index, 0, &character.to_string()));
        }
        write(db.splice_text("source", 7, 3, ""));
        write(db.set_value("stops", json!([])));
        for index in 0..5 {
            write(db.insert_list_item(
                "stops",
                index,
                json!({"position": index as f64 / 4.0, "color": format!("#00000{}", index)}),
            ));
        }
        write(db.delete_path("stops.2").map(|_| ()));
        for revision in 0..40 {
            write(db.set_path(
                "revised",
                ScalarValue::Str(format!("revision {}", revision).into()),
            ));
        }
        write(db.set_value(
            "log",
            json!({"commands": {"a": {"label": "Tint", "forward": "{}"}}}),
        ));
        db
    }

    fn whole(db: &SwirlDB) -> serde_json::Value {
        json!({
            "source": db.get_value("source"),
            "stops": db.get_value("stops"),
            "revised": db.get_value("revised"),
            "log": db.get_value("log"),
        })
    }

    #[test]
    fn a_compacted_document_reads_the_same_and_carries_one_change() {
        let db = a_worked_document();
        let before = whole(&db);
        let history = db.history();
        assert!(history.changes > 60, "{:?}", history);
        assert!(!db.is_compacted());

        let compaction = db.compact().unwrap();

        assert_eq!(whole(&db), before);
        assert_eq!(compaction.before, history);
        assert_eq!(compaction.after.changes, 1);
        assert!(compaction.after.bytes < history.bytes);
        assert_eq!(compaction.after.snapshot_bytes, compaction.after.bytes);
        assert_eq!(compaction.after.bytes_since_compaction(), 0);
        assert_eq!(history.snapshot_bytes, 0);
        assert_eq!(db.history(), compaction.after);
        assert!(db.is_compacted());
        assert_eq!(db.get_value("source"), Some(json!("export render() {}")));
    }

    #[test]
    fn a_compacted_document_survives_save_and_load_and_goes_on_taking_edits() {
        let db = a_worked_document();
        db.compact().unwrap();
        let reloaded = SwirlDB::new();
        reloaded.load_state(&db.save_state()).unwrap();
        assert_eq!(whole(&reloaded), whole(&db));
        assert!(reloaded.is_compacted());

        // A client that opens it afresh gets the snapshot, edits, and the
        // edit applies on the server with nothing missing.
        let client = SwirlDB::new();
        client.apply_changes(db.get_changes()).unwrap();
        let heads = client.get_heads();
        client.splice_text("source", 0, 0, "// hello\n").unwrap();
        client
            .insert_list_item("stops", 0, json!({"position": 0.0, "color": "#ffffff"}))
            .unwrap();
        let edits = client.local_changes_since(&heads);
        assert!(db.missing_dependencies(&edits).unwrap().is_empty());
        db.apply_changes(edits).unwrap();
        assert_eq!(whole(&db), whole(&client));
    }

    #[test]
    fn a_local_write_after_compaction_is_the_instances_own_and_nothing_else() {
        let db = a_worked_document();
        db.compact().unwrap();
        assert!(db.local_changes_since(&[]).is_empty());
        db.set_path("revised", ScalarValue::Str("after".into()))
            .unwrap();
        assert_eq!(db.local_changes_since(&[]).len(), 1);
    }

    #[test]
    fn compacting_an_empty_document_leaves_it_empty() {
        let db = SwirlDB::new();
        let compaction = db.compact().unwrap();
        assert_eq!(compaction.after, History::default());
        assert!(db.get_changes().is_empty());
        assert!(!db.is_compacted());
    }

    /// The case the server's refusals exist for: a peer that holds the
    /// history from before the compaction.
    #[test]
    fn a_peer_holding_the_history_from_before_a_compaction_cannot_sync_with_it() {
        let server = a_worked_document();
        let peer = SwirlDB::new();
        peer.apply_changes(server.get_changes()).unwrap();
        let peer_heads = peer.get_heads();
        assert!(server.unknown_heads(&peer_heads).is_empty());

        server.compact().unwrap();

        // Its heads name changes the server no longer has...
        assert_eq!(server.unknown_heads(&peer_heads), peer_heads);
        // ...so Automerge, asked for what the peer lacks, ignores the heads it
        // does not know and answers with everything: the whole snapshot, which
        // shares no change with the peer's history.
        assert_eq!(server.get_changes_since(&peer_heads), server.get_changes());

        // And what the peer writes next depends on history that is gone.
        peer.splice_text("source", 0, 0, "// lost\n").unwrap();
        let write = peer.local_changes_since(&peer_heads);
        assert_eq!(server.missing_dependencies(&write).unwrap(), peer_heads);

        // Applied anyway, Automerge does not fail: it queues the change for
        // dependencies that will never arrive, and the write is silently lost.
        // This is why the server refuses such a push instead of acknowledging it.
        let before = server.get_value("source");
        server.apply_changes(write).unwrap();
        assert_eq!(server.get_value("source"), before);
    }

    #[test]
    fn a_threshold_is_due_at_either_bound_and_never_for_one_change() {
        let threshold = CompactionThreshold {
            changes: 10,
            bytes: 1_000,
        };
        let history = |changes, bytes, snapshot_bytes| History {
            changes,
            bytes,
            snapshot_bytes,
        };
        assert!(!threshold.is_due(history(9, 999, 0)));
        assert!(threshold.is_due(history(10, 1, 0)));
        assert!(threshold.is_due(history(2, 1_000, 0)));
        assert!(!threshold.is_due(history(1, 1_000_000, 0)));
        assert!(!threshold.is_due(History::default()));
        // A state heavier than the bound is not history: only what was
        // written after it counts.
        assert!(!threshold.is_due(history(2, 5_999, 5_000)));
        assert!(threshold.is_due(history(2, 6_000, 5_000)));
    }

    #[test]
    fn a_snapshot_keeps_every_kind_of_value() {
        let mut document = AutoCommit::new();
        let map = document.put_object(ROOT, "map", ObjType::Map).unwrap();
        document.put(&map, "int", -3_i64).unwrap();
        document.put(&map, "uint", ScalarValue::Uint(7)).unwrap();
        document.put(&map, "float", 0.25_f64).unwrap();
        document.put(&map, "bool", true).unwrap();
        document.put(&map, "null", ScalarValue::Null).unwrap();
        document
            .put(&map, "bytes", ScalarValue::Bytes(vec![1, 2, 3]))
            .unwrap();
        document
            .put(&map, "timestamp", ScalarValue::Timestamp(1_789_000_000))
            .unwrap();
        document
            .put(&map, "counter", ScalarValue::counter(10))
            .unwrap();
        document.increment(&map, "counter", 5).unwrap();
        let list = document.put_object(ROOT, "list", ObjType::List).unwrap();
        document.insert(&list, 0, "first").unwrap();
        let nested = document.insert_object(&list, 1, ObjType::List).unwrap();
        document.insert(&nested, 0, 42_i64).unwrap();
        let text = document.put_object(ROOT, "text", ObjType::Text).unwrap();
        document.splice_text(&text, 0, 0, "héllo 🌀").unwrap();
        document.commit();

        let mut snapshot = snapshot(&document, TextEncoding::Utf16CodeUnit).unwrap();

        assert_eq!(history(&mut snapshot).changes, 1);
        assert!(is_compacted(&mut snapshot));
        let map_id = snapshot.get(ROOT, "map").unwrap().unwrap().1;
        let read = |key: &str| {
            snapshot
                .get(&map_id, key)
                .unwrap()
                .unwrap()
                .0
                .into_scalar()
                .unwrap()
        };
        assert_eq!(read("int"), ScalarValue::Int(-3));
        assert_eq!(read("uint"), ScalarValue::Uint(7));
        assert_eq!(read("float"), ScalarValue::F64(0.25));
        assert_eq!(read("bool"), ScalarValue::Boolean(true));
        assert_eq!(read("null"), ScalarValue::Null);
        assert_eq!(read("bytes"), ScalarValue::Bytes(vec![1, 2, 3]));
        assert_eq!(read("timestamp"), ScalarValue::Timestamp(1_789_000_000));
        let ScalarValue::Counter(counter) = read("counter") else {
            panic!("a counter stays a counter");
        };
        assert_eq!(i64::from(&counter), 15);
        snapshot.increment(&map_id, "counter", 1).unwrap();
        let ScalarValue::Counter(counter) = snapshot
            .get(&map_id, "counter")
            .unwrap()
            .unwrap()
            .0
            .into_scalar()
            .unwrap()
        else {
            panic!("a counter stays a counter");
        };
        assert_eq!(i64::from(&counter), 16);

        let list_id = snapshot.get(ROOT, "list").unwrap().unwrap().1;
        assert_eq!(snapshot.length(&list_id), 2);
        let nested_id = snapshot.get(&list_id, 1).unwrap().unwrap().1;
        assert_eq!(
            snapshot
                .get(&nested_id, 0)
                .unwrap()
                .unwrap()
                .0
                .into_scalar()
                .unwrap(),
            ScalarValue::Int(42)
        );
        let text_id = snapshot.get(ROOT, "text").unwrap().unwrap().1;
        assert_eq!(snapshot.text(&text_id).unwrap(), "héllo 🌀");
    }
}
