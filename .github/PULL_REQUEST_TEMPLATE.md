<!--
Thanks for the PR! A few notes to keep things smooth:

- Title: use imperative mood, no trailing period
  (e.g. "Add keepalive sleeper trait" — not "Added keepalive...")
- One logical change per PR. Big refactors are easier to review when
  split into a series.
- For protocol or API changes, link the relevant design doc in
  `design/` or call out where the discussion happened.
- Delete sections that don't apply.
-->

## Summary

<!--
What does this PR change, and why? Lead with the motivation — the
"what" is visible in the diff; the "why" is what reviewers need.
1–3 bullet points is ideal.
-->

-

## Related issues

<!--
Link issues this addresses. Use `Fixes #123` to auto-close on merge,
or `Refs #123` for related-but-not-closing.
-->

-

## Type of change

<!-- Check all that apply. -->

- [ ] Bug fix (non-breaking)
- [ ] New feature (non-breaking)
- [ ] Breaking change (API, wire format, or behavior)
- [ ] Performance improvement
- [ ] Refactor / cleanup (no behavior change)
- [ ] Documentation
- [ ] CI / build / tooling
- [ ] Dependency bump

## How was this tested?

<!--
Concrete steps: which tests / benches / manual runs. If you added
new test cases, mention them; if behavior is hard to test, explain
why.
-->

-

## Breaking changes

<!--
If you checked "Breaking change" above, describe the break and the
migration path here. Include before/after snippets where useful.
Delete this section otherwise.
-->

## Checklist

- [ ] **I have personally reviewed every line of this diff.** I have read the code, understand what each change does, and am willing to defend it on its merits. AI-assisted authoring is welcome; unreviewed AI output is not.
- [ ] `cargo build --workspace --all-features` succeeds
- [ ] `cargo test --workspace` succeeds (or relevant subset, with rationale)
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` is clean
- [ ] `cargo +nightly fmt --all -- --check` is clean
- [ ] Public API changes have doc comments
- [ ] User-visible changes are reflected in the relevant `README.md` or `design/` doc
- [ ] Cohort version bumps applied if this is a release-blocking change
