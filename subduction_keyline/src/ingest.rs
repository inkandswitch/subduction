//! Admit a received batch into a [`Keyline`] store.
//!
//! The store accepts any certificate, including ones that are dead on
//! arrival. The tree that carries certificates only grows, so anything
//! admitted here is replicated forever. [`admit`] keeps out certificates
//! whose issuer has no place in the graph:
//!
//! | Certificate | Admitted when the issuer…                                              |
//! |-------------|-------------------------------------------------------------------------|
//! | Delegation  | has positive standing over its subject, at any level                   |
//! | Revocation  | issued or received the target, or has positive standing over its subject |
//!
//! These mirror Keyline's own rules. Issuers need standing "at any level;
//! levels clamp, they do not gate". Any well-signed revocation is valid, but
//! one can only take effect through the issuer's admin reach (which implies
//! standing over the target's subject) or by one of the target's parties.
//! So every certificate that could ever take effect is admitted. Only
//! certificates from outside the graph are refused.
//!
//! Standing is [`Keyline::positive_power`], which ignores revocations and
//! only grows as the set grows. A refusal therefore only means _not yet_: a
//! later sync that delivers the missing certificates turns it into an
//! admission, and every replica reaches the same decision.

use alloc::{collections::BTreeMap, vec::Vec};

use future_form::FutureForm;
use keyhive_crypto::{digest::Digest, identifier::Identifier, verified::VerifyError};
use keyline::{
    certificate::{Certificate, CertificateId, VerifiedCertificate},
    contract::{Keyline, Version},
    delegation::Delegation,
};

use crate::batch::Batch;

/// A batch that is now wholly in the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Admitted {
    version: Version,
    inserted: usize,
}

impl Admitted {
    /// The store version after the last insert.
    #[must_use]
    pub const fn version(&self) -> Version {
        self.version
    }

    /// How many certificates were new to the store.
    #[must_use]
    pub const fn inserted(&self) -> usize {
        self.inserted
    }
}

/// Admit `batch` into `keyline`.
///
/// Certificates the store already holds are skipped before their signatures
/// are checked. The rest are verified and then admitted in rounds. Each
/// round inserts, atomically, every certificate whose issuer has standing
/// against the store as it is. In the common case one round admits the whole
/// batch, so the batch is one atomic insert. Later rounds pick up
/// certificates whose proofs came earlier in the same batch.
///
/// If some certificates still lack standing, the ones that had it stay
/// inserted. Each was admissible on its own, and admission only ever grows.
///
/// # Errors
///
/// - [`AdmitError::InvalidCertificate`]: a certificate failed verification.
///   It never will pass, so the batch is refused for good.
/// - [`AdmitError::NotYet`]: some issuers have no standing yet. Retry after
///   more certificates arrive.
/// - [`AdmitError::Storage`]: the store failed.
pub async fn admit<F: FutureForm, K: Keyline<F>>(
    keyline: &K,
    batch: &Batch<K::RetentionWatermark>,
) -> Result<Admitted, AdmitError<K::Error>> {
    let mut pending = Vec::with_capacity(batch.len());
    for cert in batch.certificates() {
        let id = cert.id();
        if !contains(keyline, id).await? {
            let verified = cert
                .clone()
                .verify()
                .map_err(|source| AdmitError::InvalidCertificate { id, source })?;
            pending.push(verified);
        }
    }

    // Delegations in this batch, by digest, for revocations that name them.
    let local: BTreeMap<Digest<Delegation>, (Identifier, Delegation)> = pending
        .iter()
        .filter_map(|cert| match cert {
            VerifiedCertificate::Delegation(d) => Some((d.digest(), (d.issuer(), *d.payload()))),
            VerifiedCertificate::Revocation(_) => None,
        })
        .collect();

    let mut version = keyline.version().await.map_err(AdmitError::Storage)?;
    let mut inserted = 0;
    while !pending.is_empty() {
        let mut ready = Vec::new();
        let mut waiting = Vec::new();
        for cert in pending {
            if has_standing(keyline, &cert, &local).await? {
                ready.push(cert);
            } else {
                waiting.push(cert);
            }
        }

        if ready.is_empty() {
            return Err(AdmitError::NotYet {
                waiting: waiting.iter().map(VerifiedCertificate::id).collect(),
            });
        }

        inserted += ready.len();
        version = keyline.insert(ready).await.map_err(AdmitError::Storage)?;
        pending = waiting;
    }

    Ok(Admitted { version, inserted })
}

async fn contains<F: FutureForm, K: Keyline<F>>(
    keyline: &K,
    id: CertificateId,
) -> Result<bool, AdmitError<K::Error>> {
    Ok(keyline
        .contains(id)
        .await
        .map_err(AdmitError::Storage)?
        .value)
}

async fn standing<F: FutureForm, K: Keyline<F>>(
    keyline: &K,
    subject: Identifier,
    issuer: Identifier,
) -> Result<bool, AdmitError<K::Error>> {
    Ok(keyline
        .positive_power(subject, issuer)
        .await
        .map_err(AdmitError::Storage)?
        .value
        .is_some())
}

async fn has_standing<F: FutureForm, K: Keyline<F>>(
    keyline: &K,
    cert: &VerifiedCertificate<K::RetentionWatermark>,
    local: &BTreeMap<Digest<Delegation>, (Identifier, Delegation)>,
) -> Result<bool, AdmitError<K::Error>> {
    match cert {
        VerifiedCertificate::Delegation(d) => {
            standing(keyline, d.payload().subject, d.issuer()).await
        }
        VerifiedCertificate::Revocation(r) => {
            let target = r.payload().revoke;
            let Some((target_issuer, target)) = target_delegation(keyline, target, local).await?
            else {
                return Ok(false);
            };

            let issuer = r.issuer();
            Ok(issuer == target_issuer
                || issuer == target.audience
                || standing(keyline, target.subject, issuer).await?)
        }
    }
}

/// The delegation a revocation names, with its issuer: from this batch, or
/// from the store. `None` if neither holds it yet.
async fn target_delegation<F: FutureForm, K: Keyline<F>>(
    keyline: &K,
    target: Digest<Delegation>,
    local: &BTreeMap<Digest<Delegation>, (Identifier, Delegation)>,
) -> Result<Option<(Identifier, Delegation)>, AdmitError<K::Error>> {
    if let Some(found) = local.get(&target) {
        return Ok(Some(*found));
    }

    let stored = keyline
        .get(CertificateId::Delegation(target))
        .await
        .map_err(AdmitError::Storage)?
        .value;

    match stored {
        Some(Certificate::Delegation(signed)) => {
            // The store holds verified certificates, so the bytes are
            // canonical. A decode failure means a corrupt store.
            let delegation = signed
                .encoded()
                .decode()
                .map_err(|_| AdmitError::CorruptStore(CertificateId::Delegation(target)))?;
            Ok(Some((signed.issuer(), delegation)))
        }
        Some(Certificate::Revocation(_)) | None => Ok(None),
    }
}

/// Why a batch was not admitted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmitError<E> {
    /// A certificate failed verification. It never will pass.
    #[error("certificate {id:?} failed verification")]
    InvalidCertificate {
        /// The certificate that failed.
        id: CertificateId,

        /// Why it failed.
        #[source]
        source: VerifyError,
    },

    /// Some issuers have no standing yet. Retry once more certificates have
    /// arrived.
    #[error("{} certificate(s) have no standing yet", waiting.len())]
    NotYet {
        /// The certificates still waiting, ascending by ID.
        waiting: Vec<CertificateId>,
    },

    /// The store holds a delegation whose bytes do not decode.
    #[error("store holds a corrupt certificate {0:?}")]
    CorruptStore(CertificateId),

    /// The Keyline store failed.
    #[error("keyline store failed")]
    Storage(#[source] E),
}

impl<E> AdmitError<E> {
    /// Whether retrying after more certificates arrive could succeed.
    #[must_use]
    pub const fn is_not_yet(&self) -> bool {
        matches!(self, Self::NotYet { .. })
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]
mod tests {
    use super::*;

    use alloc::vec;

    use future_form::Sendable;
    use futures::executor::block_on;
    use keyhive_crypto::verified::Verified;
    use keyline::{
        power::Power,
        revocation::Revocation,
        test_utils::{assume_verified, delegation, id, signing_key},
    };
    use keyline_memory::MemoryKeyline;
    use testresult::TestResult;

    type W = ();
    type Store = MemoryKeyline<W>;

    fn grant(issuer: u8, audience: u8, subject: u8, power: Power) -> Certificate<W> {
        let payload = Delegation::new(id(audience), id(subject), power);
        VerifiedCertificate::from(
            Verified::sign_sync(&payload, &signing_key(issuer)).expect("signs"),
        )
        .into_certificate()
    }

    fn revoke(issuer: u8, target: &Certificate<W>) -> Certificate<W> {
        let CertificateId::Delegation(digest) = target.id() else {
            panic!("only delegations can be revoked");
        };
        let payload = Revocation::<W>::new(digest);
        VerifiedCertificate::from(
            Verified::sign_sync(&payload, &signing_key(issuer)).expect("signs"),
        )
        .into_certificate()
    }

    fn batch<I: IntoIterator<Item = Certificate<W>>>(certs: I) -> Batch<W> {
        Batch::new(certs).expect("non-empty")
    }

    fn run(
        store: &Store,
        certs: Vec<Certificate<W>>,
    ) -> Result<Admitted, AdmitError<core::convert::Infallible>> {
        block_on(admit::<Sendable, _>(store, &batch(certs)))
    }

    fn holds(store: &Store, cert: &Certificate<W>) -> bool {
        block_on(<Store as Keyline<Sendable>>::contains(store, cert.id()))
            .expect("infallible")
            .value
    }

    #[test]
    fn admits_root_edges_from_an_empty_store() -> TestResult {
        let store = Store::new();
        let root = grant(1, 2, 1, Power::Admin);

        assert_eq!(run(&store, vec![root.clone()])?.inserted(), 1);
        assert!(holds(&store, &root));
        Ok(())
    }

    #[test]
    fn outsider_waits_until_its_standing_arrives() -> TestResult {
        let store = Store::new();
        let root = grant(1, 2, 1, Power::Edit);
        let onward = grant(2, 3, 1, Power::Read);

        assert_eq!(
            run(&store, vec![onward.clone()]),
            Err(AdmitError::NotYet {
                waiting: vec![onward.id()]
            })
        );
        assert!(!holds(&store, &onward));

        run(&store, vec![root])?;
        assert_eq!(run(&store, vec![onward.clone()])?.inserted(), 1);
        assert!(holds(&store, &onward));
        Ok(())
    }

    #[test]
    fn proofs_earlier_in_the_same_batch_count() -> TestResult {
        let store = Store::new();
        let root = grant(1, 2, 1, Power::Admin);
        let onward = grant(2, 3, 1, Power::Read);
        let further = grant(3, 4, 1, Power::Read);

        let admitted = run(&store, vec![further.clone(), onward.clone(), root.clone()])?;
        assert_eq!(admitted.inserted(), 3);
        assert!([root, onward, further].iter().all(|c| holds(&store, c)));
        Ok(())
    }

    #[test]
    fn rejects_bad_signatures_for_good() {
        let store = Store::new();
        let forged = assume_verified::<W, _>(delegation(id(1), id(2), id(1), Power::Admin))
            .into_certificate();

        assert!(matches!(
            run(&store, vec![forged]),
            Err(AdmitError::InvalidCertificate {
                source: VerifyError::BadSignature,
                ..
            })
        ));
    }

    #[test]
    fn known_certificates_skip_verification() -> TestResult {
        let store = Store::new();
        let root = grant(1, 2, 1, Power::Admin);
        run(&store, vec![root])?;

        // Same statement (same ID), but a zero signature: never checked,
        // because the store already holds it.
        let resent = assume_verified::<W, _>(delegation(id(1), id(2), id(1), Power::Admin))
            .into_certificate();
        assert_eq!(run(&store, vec![resent])?.inserted(), 0);
        Ok(())
    }

    #[test]
    fn revocations_need_a_target_and_a_relation_to_it() -> TestResult {
        let store = Store::new();
        let root = grant(1, 2, 1, Power::Admin);
        let member = grant(2, 3, 1, Power::Read);

        // Target unknown: not yet.
        let early = revoke(2, &member);
        assert!(run(&store, vec![early.clone()]).is_err_and(|e| e.is_not_yet()));

        run(&store, vec![root, member.clone()])?;

        // An outsider's revocation stays out.
        let outsider = revoke(9, &member);
        assert!(run(&store, vec![outsider]).is_err_and(|e| e.is_not_yet()));

        // The target's issuer, its audience, and anyone with standing over
        // the subject all get in.
        run(&store, vec![early])?;
        run(&store, vec![revoke(3, &member)])?;
        run(&store, vec![revoke(1, &member)])?;
        Ok(())
    }

    #[test]
    fn revocation_of_a_delegation_in_the_same_batch() -> TestResult {
        let store = Store::new();
        let root = grant(1, 2, 1, Power::Admin);
        let member = grant(2, 3, 1, Power::Read);
        let removed = revoke(2, &member);

        assert_eq!(run(&store, vec![root, member, removed])?.inserted(), 3);
        Ok(())
    }

    /// Two replicas that receive the same batches in different orders, and
    /// retry refused batches until nothing changes, end with the same set.
    #[test]
    fn admission_converges_regardless_of_order() {
        const IDENTITIES: u8 = 5;
        let ident = |n: u8| n % IDENTITIES + 1;

        bolero::check!()
            .with_type::<(Vec<(u8, u8, u8, u8, u8)>, Vec<(u8, u8, u8)>, u8)>()
            .for_each(|(grants, revokes, rotate)| {
                let delegations: Vec<(u8, Certificate<W>)> = grants
                    .iter()
                    .map(|&(issuer, audience, subject, power, batch)| {
                        (
                            batch % 4,
                            grant(
                                ident(issuer),
                                ident(audience),
                                ident(subject),
                                Power::ALL[usize::from(power % 4)],
                            ),
                        )
                    })
                    .collect();
                let revocations = revokes.iter().filter_map(|&(issuer, target, batch)| {
                    let (_, target) =
                        delegations.get(usize::from(target) % delegations.len().max(1))?;
                    Some((batch % 4, revoke(ident(issuer), target)))
                });

                let mut by_batch: BTreeMap<u8, Vec<Certificate<W>>> = BTreeMap::new();
                for (b, cert) in delegations.iter().cloned().chain(revocations) {
                    by_batch.entry(b).or_default().push(cert);
                }
                let batches: Vec<Vec<Certificate<W>>> = by_batch.into_values().collect();
                if batches.is_empty() {
                    return;
                }

                let mut reordered = batches.clone();
                let by = usize::from(*rotate) % reordered.len();
                reordered.rotate_left(by);
                reordered.reverse();

                let settle = |order: &[Vec<Certificate<W>>]| {
                    let store = Store::new();
                    loop {
                        let before = block_on(<Store as Keyline<Sendable>>::version(&store))
                            .expect("infallible");
                        for certs in order {
                            // Refusals are expected; the loop retries them.
                            drop(run(&store, certs.clone()));
                        }
                        let after = block_on(<Store as Keyline<Sendable>>::version(&store))
                            .expect("infallible");
                        if before == after {
                            break;
                        }
                    }
                    block_on(<Store as Keyline<Sendable>>::digest(&store))
                        .expect("infallible")
                        .value
                };

                assert_eq!(settle(&batches), settle(&reordered));
            });
    }
}
