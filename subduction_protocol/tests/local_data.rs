//! Local data commands: hydration, authored commits (fused seal+persist),
//! tree removal, and the in-flight interleavings between them.

use std::collections::BTreeSet;

use ed25519_dalek::SigningKey;
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use subduction_protocol::{
    command::{Command, NewCommit},
    effect::{AppEvent, Effect},
    event::Event,
    machine::{Config, Machine, Now},
    outcome::{IgnoreReason, Outcome},
    peer_id::PeerId,
    storage::{StorageOp, StorageResult},
    ticket::{Entity, StorageTicket},
    timestamp::Timestamp,
    wall_clock::TimestampSeconds,
};
use testresult::TestResult;

const fn now() -> Now {
    Now {
        monotonic: Timestamp::from_millis(0),
        wall: TimestampSeconds::new(1_700_000_000),
    }
}

fn machine() -> (Machine, MemorySigner) {
    let key = SigningKey::from_bytes(&[42u8; 32]);
    let signer = MemorySigner::from_bytes(&[42u8; 32]);
    let local_peer = PeerId::from(key.verifying_key());
    (Machine::new(Config::new(local_peer, [7u8; 32])), signer)
}

const fn tree() -> SedimentreeId {
    SedimentreeId::new([1u8; 32])
}

fn commit(tree: SedimentreeId, head: u8, parents: &[u8]) -> LooseCommit {
    LooseCommit::new(
        tree,
        CommitId::new([head; 32]),
        parents.iter().map(|b| CommitId::new([*b; 32])).collect(),
        BlobMeta::new(&Blob::new(vec![head; 8])),
    )
}

/// Simulate the driver executing an `IngestLocal`: hash, build, seal.
fn execute_ingest_local(
    signer: &MemorySigner,
    tree: SedimentreeId,
    commits: &[NewCommit],
) -> StorageResult {
    let sealed = commits
        .iter()
        .map(|new| {
            let commit = LooseCommit::new(
                tree,
                new.head,
                new.parents.clone(),
                BlobMeta::new(&new.blob),
            );
            futures::executor::block_on(Signed::seal::<Sendable, _>(signer, commit)).into_signed()
        })
        .collect();
    StorageResult::LocallyIngested { commits: sealed }
}

#[test]
fn hydration_installs_metadata() {
    let (mut machine, _signer) = machine();
    let t = tree();

    let outcome = machine.handle(
        now(),
        Event::Command(Command::HydrateTree {
            tree: t,
            commits: vec![commit(t, 1, &[]), commit(t, 2, &[1])],
            fragments: vec![],
        }),
    );
    assert_eq!(outcome, Outcome::Progressed);
    assert_eq!(machine.poll_effect(), None, "hydration produces no effects");
    assert_eq!(machine.tree_ids().collect::<Vec<_>>(), [t]);
    assert_eq!(machine.tree_heads(t), Some(vec![CommitId::new([2u8; 32])]));
}

#[test]
fn add_commits_updates_resident_only_after_durability() -> TestResult {
    let (mut machine, signer) = machine();
    let t = tree();
    let new = NewCommit {
        head: CommitId::new([9u8; 32]),
        parents: BTreeSet::new(),
        blob: Blob::new(vec![9u8; 8]),
    };

    let outcome = machine.handle(
        now(),
        Event::Command(Command::AddCommits {
            tree: t,
            commits: vec![new],
        }),
    );
    assert_eq!(outcome, Outcome::Progressed);

    // The fused op went to the driver; nothing is resident yet.
    let Some(Effect::Storage { ticket, op }) = machine.poll_effect() else {
        return Err("expected a storage effect".into());
    };
    assert_eq!(ticket.entity, Entity::Local);
    assert_eq!(
        machine.tree_heads(t),
        None,
        "not resident before durability"
    );

    let StorageOp::IngestLocal {
        tree: op_tree,
        commits,
    } = op
    else {
        return Err("expected IngestLocal".into());
    };
    assert_eq!(op_tree, t);

    // Driver completes: seal + persist.
    let result = execute_ingest_local(&signer, t, &commits);
    let outcome = machine.handle(now(), Event::StorageDone { ticket, result });
    assert_eq!(outcome, Outcome::Progressed);

    assert_eq!(machine.tree_heads(t), Some(vec![CommitId::new([9u8; 32])]));
    let Some(Effect::App(AppEvent::CommitsStored {
        tree: event_tree,
        heads,
    })) = machine.poll_effect()
    else {
        return Err("expected CommitsStored".into());
    };
    assert_eq!(event_tree, t);
    assert_eq!(heads, [CommitId::new([9u8; 32])]);
    Ok(())
}

#[test]
fn remove_tree_round_trips() -> TestResult {
    let (mut machine, _signer) = machine();
    let t = tree();

    let _outcome = machine.handle(
        now(),
        Event::Command(Command::HydrateTree {
            tree: t,
            commits: vec![commit(t, 1, &[])],
            fragments: vec![],
        }),
    );

    let outcome = machine.handle(now(), Event::Command(Command::RemoveTree { tree: t }));
    assert_eq!(outcome, Outcome::Progressed);
    assert_eq!(
        machine.tree_heads(t),
        None,
        "resident state gone immediately"
    );

    let Some(Effect::Storage { ticket, op }) = machine.poll_effect() else {
        return Err("expected a storage effect".into());
    };
    assert!(matches!(op, StorageOp::DeleteTree { tree, .. } if tree == t));

    let outcome = machine.handle(
        now(),
        Event::StorageDone {
            ticket,
            result: StorageResult::TreeDeleted,
        },
    );
    assert_eq!(outcome, Outcome::Progressed);
    assert_eq!(
        machine.poll_effect(),
        Some(Effect::App(AppEvent::TreeRemoved { tree: t }))
    );
    Ok(())
}

#[test]
fn ingest_completion_after_remove_is_dropped() -> TestResult {
    let (mut machine, signer) = machine();
    let t = tree();
    let new = NewCommit {
        head: CommitId::new([9u8; 32]),
        parents: BTreeSet::new(),
        blob: Blob::new(vec![9u8; 8]),
    };

    // Ingest goes out…
    let _outcome = machine.handle(
        now(),
        Event::Command(Command::AddCommits {
            tree: t,
            commits: vec![new],
        }),
    );
    let Some(Effect::Storage { ticket, op }) = machine.poll_effect() else {
        return Err("expected ingest effect".into());
    };
    let StorageOp::IngestLocal { commits, .. } = op else {
        return Err("expected IngestLocal".into());
    };

    // …but the app removes the tree while the write is in flight.
    let _outcome = machine.handle(now(), Event::Command(Command::RemoveTree { tree: t }));
    let Some(Effect::Storage { .. }) = machine.poll_effect() else {
        return Err("expected delete effect".into());
    };

    // The ingest completion lands after the removal decision: dropped.
    let result = execute_ingest_local(&signer, t, &commits);
    let outcome = machine.handle(now(), Event::StorageDone { ticket, result });
    assert_eq!(outcome, Outcome::Ignored(IgnoreReason::StaleTicket));
    assert_eq!(machine.tree_heads(t), None, "removed tree stays removed");
    Ok(())
}

#[test]
fn mutated_local_ticket_is_ignored() -> TestResult {
    let (mut machine, signer) = machine();
    let t = tree();
    let new = NewCommit {
        head: CommitId::new([9u8; 32]),
        parents: BTreeSet::new(),
        blob: Blob::new(vec![9u8; 8]),
    };

    let _outcome = machine.handle(
        now(),
        Event::Command(Command::AddCommits {
            tree: t,
            commits: vec![new],
        }),
    );
    let Some(Effect::Storage { ticket, op }) = machine.poll_effect() else {
        return Err("expected ingest effect".into());
    };
    let StorageOp::IngestLocal { commits, .. } = op else {
        return Err("expected IngestLocal".into());
    };

    // Wrong seq: nothing pends under it.
    let wrong = StorageTicket {
        seq: ticket.seq.next(),
        ..ticket
    };
    let result = execute_ingest_local(&signer, t, &commits);
    let outcome = machine.handle(
        now(),
        Event::StorageDone {
            ticket: wrong,
            result,
        },
    );
    assert_eq!(outcome, Outcome::Ignored(IgnoreReason::UnknownTicket));
    assert_eq!(machine.tree_heads(t), None);
    Ok(())
}
