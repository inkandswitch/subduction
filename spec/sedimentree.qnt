// Sedimentree v2: merged commits/fragments, no checkpoints, tombstones on over-sync.
//
// Models
//   - One item per commit; level = depth(head). Level 0 is a plain commit.
//   - Minimize by a DAG walk from headParents: all or nothing, or partial (WALK).
//   - Sync as an atomic push of any subset of items the peer doesn't advertise
//     (partial subsets stand in for interrupted syncs).
//   - Optionally, items are accepted only if their boundary is already covered (CONNECTED).
//   - Tombstones issued by completed walks, sent back only when a peer over-syncs.
//     The receiver suppresses that peer, adopts and drops, or quarantines (MODE),
//     believing any peer or only the covering item's author (TRUST).
//   - Authors sign tombstones for every commit in their new item's range.
//   - Quarantined items can still be sent, just not advertised (SERVE_QUARANTINED).
//   - Byzantine peers reply with forged tombstones.
//
// Does NOT model
//   - Blobs that lie about their own range, or forged headParents (both are trust
//     assumptions here; headParents is a claim unless checkable against the head hash).
//   - Two different items with the same head.
//   - Fingerprints, message interleaving, signatures, access control.
//   - Fairness: settledConvergence checks what a full exchange between two honest
//     nodes would produce, rather than proving that one eventually happens.
//
// Run:  quint run sedimentree.qnt --main=default --invariant <name>
//       ./matrix.sh   (every configuration; see the script for knobs)

module modes {
  /// What a node does with a tombstone it receives after over-syncing.
  type TombstoneMode =
    | SuppressPeer // stop sending that item to that peer; keep everything
    | Adopt        // store the tombstone (forwardable) and drop the item if it holds the coverer
    | Quarantine   // as Adopt, but keep the bytes: stop advertising and sending the item

  /// Whose tombstones a sender believes.
  type TrustMode =
    | AnyPeer    // any peer's tombstone
    | AuthorOnly // only one signed by the author of the covering item

  /// How minimize treats gaps on disk during a walk.
  type WalkMode =
    | AllOrNothing // any gap keeps everything
    | Partial      // drop what the walk reached through held items, gaps notwithstanding
}

module sedimentree {
  import modes.*

  // ── Parameters ───────────────────────────────────────────────────────────

  type NodeId = str
  type CommitId = int
  type Depth = int

  const NODES: Set[NodeId]
  const BYZANTINE: Set[NodeId]  // subset of NODES
  const MAX_COMMITS: int        // bounds the DAG, and every walk
  const MAX_DEPTH: Depth        // depths drawn from 0..MAX_DEPTH
  const MODE: TombstoneMode
  const WALK: WalkMode
  const CONNECTED: bool         // accept an item only if its boundary is already covered
  const SETTLE_ROUNDS: int      // exchange rounds used by settledConvergence
  const TRUST: TrustMode
  const SERVE_QUARANTINED: bool // quarantined items are not advertised, but are still sent

  pure val HONEST: Set[NodeId] = NODES.exclude(BYZANTINE)

  // ── Ghost: the true commit DAG (what the hashes would tell you) ─────────

  type CommitInfo = {
    parents: Set[CommitId],
    depth: Depth,
  }

  // ── Items: commits and fragments are the same thing ─────────────────────
  //
  // An item's level is depth(head). It covers head plus every ancestor reachable
  // without passing through a commit of depth >= level; those stopping commits
  // are its boundary. At level 0 the boundary is exactly the head's parents.

  type Item = {
    head: CommitId,
    headParents: Set[CommitId], // lets a walk start without a separate depth-0 item
    boundary: Set[CommitId],
    content: Set[CommitId],     // ghost: what the opaque blob really holds
    author: NodeId,             // who signed it
  }

  /// "covered is inside by's range", signed by `issuer`: the author of `by` when it
  /// writes it, or any node that completed the walk from `by`.
  type Tombstone = {
    covered: CommitId,
    by: CommitId,
    issuer: NodeId,
  }

  type LocalState = {
    items: Set[Item],                    // live: advertised, sent, minimized
    quarantined: Set[Item],              // kept on disk, otherwise ignored
    tombstones: Set[Tombstone],
    suppressed: Set[(NodeId, CommitId)], // (peer, item head) not to send again
  }

  type WalkStatus = Complete | Incomplete | Malformed

  // ── State ────────────────────────────────────────────────────────────────

  var dag: CommitId -> CommitInfo          // ghost, global
  var nodes: NodeId -> LocalState
  var everCovered: NodeId -> Set[CommitId] // ghost: for "coverage never shrinks"
  var oversyncs: int                       // ghost: items sent that the receiver already covered
  var replies: int                         // ghost: tombstones sent back after an over-sync

  // ── Functional layer ─────────────────────────────────────────────────────

  pure def depthOf(g: CommitId -> CommitInfo, c: CommitId): Depth = g.get(c).depth

  pure def headsOf(items: Set[Item]): Set[CommitId] = items.map(i => i.head)

  pure def coveredBy(items: Set[Item]): Set[CommitId] = items.map(i => i.content).flatten()

  /// Commits in `cs` that no other commit in `cs` has as a parent.
  pure def tipsOf(g: CommitId -> CommitInfo, cs: Set[CommitId]): Set[CommitId] =
    cs.filter(c => not(cs.exists(d => g.get(d).parents.contains(c))))

  /// From `start`, collect ancestors of depth < level (interior) and the
  /// depth >= level commits where the descent stops (boundary).
  pure def rangeOf(
    g: CommitId -> CommitInfo,
    start: Set[CommitId],
    level: Depth,
  ): { interior: Set[CommitId], boundary: Set[CommitId] } = {
    val r = 1.to(MAX_COMMITS).fold(
      { interior: Set(), boundary: Set(), frontier: start },
      (s, _) => {
        val inner = s.frontier.filter(c => depthOf(g, c) < level)
        val interior = s.interior.union(inner)
        {
          interior: interior,
          boundary: s.boundary.union(s.frontier.exclude(inner)),
          frontier: inner.map(c => g.get(c).parents).flatten().exclude(interior),
        }
      }
    )
    { interior: r.interior, boundary: r.boundary }
  }

  /// The item an honest author builds for commit `c`.
  pure def buildItem(g: CommitId -> CommitInfo, c: CommitId, author: NodeId): Item = {
    val info = g.get(c)
    val r = rangeOf(g, info.parents, info.depth)
    { head: c, headParents: info.parents, boundary: r.boundary, content: r.interior.union(Set(c)), author: author }
  }

  /// The tombstones an author can sign for its own item: every commit in its range.
  pure def authorTombstones(i: Item): Set[Tombstone] =
    i.content.exclude(Set(i.head)).map(x => { covered: x, by: i.head, issuer: i.author })

  /// The subsumption walk, using only the items on disk (plus depths, which hashes give).
  /// Follows headParents, then each held item's boundary, stopping at f.boundary.
  pure def walk(
    g: CommitId -> CommitInfo,
    items: Set[Item],
    f: Item,
  ): { status: WalkStatus, visited: Set[CommitId] } = {
    val level = depthOf(g, f.head)
    val held = headsOf(items)
    val r = 1.to(MAX_COMMITS).fold(
      { visited: Set(), reached: Set(), frontier: f.headParents, tooDeep: false, gap: false },
      (s, _) => {
        val rest = s.frontier.exclude(f.boundary)
        val deep = rest.filter(c => depthOf(g, c) >= level)
        val shallow = rest.exclude(deep)
        val hit = shallow.intersect(held)
        val visited = s.visited.union(hit)
        {
          visited: visited,
          reached: s.reached.union(s.frontier.intersect(f.boundary)),
          frontier: items.filter(i => hit.contains(i.head)).map(i => i.boundary).flatten().exclude(visited),
          tooDeep: s.tooDeep or deep.size() > 0,
          gap: s.gap or shallow.exclude(held).size() > 0,
        }
      }
    )
    val status =
      if (r.tooDeep) Malformed
      else if (r.gap and WALK == AllOrNothing) Incomplete
      else if (r.gap) Complete // Partial: everything visited is provably in range
      else if (r.reached != f.boundary) Malformed
      else Complete
    { status: status, visited: r.visited }
  }

  /// One round: every deep item whose walk is complete drops what it visited.
  /// All drops come from the same snapshot, so the round is order independent.
  pure def minimizeRound(
    g: CommitId -> CommitInfo,
    items: Set[Item],
    issuer: NodeId,
  ): { items: Set[Item], tombstones: Set[Tombstone] } = {
    val issued = items
      .filter(f => depthOf(g, f.head) > 0)
      .map(f => {
        val w = walk(g, items, f)
        if (w.status == Complete) w.visited.map(v => { covered: v, by: f.head, issuer: issuer }) else Set()
      })
      .flatten()
    val dropped = issued.map(t => t.covered)
    { items: items.filter(i => not(dropped.contains(i.head))), tombstones: issued }
  }

  pure def minimize(
    g: CommitId -> CommitInfo,
    items: Set[Item],
    issuer: NodeId,
  ): { items: Set[Item], tombstones: Set[Tombstone] } =
    1.to(MAX_COMMITS).fold(
      { items: items, tombstones: Set() },
      (s, _) => {
        val r = minimizeRound(g, s.items, issuer)
        { items: r.items, tombstones: s.tombstones.union(r.tombstones) }
      }
    )

  /// Held heads that cover `c` through a chain of tombstones (c ⊑ x ⊑ … ⊑ held).
  pure def coverers(ts: Set[Tombstone], held: Set[CommitId], c: CommitId): Set[CommitId] = {
    val chain = 1.to(MAX_COMMITS).fold(
      Set(c),
      (s, _) => s.union(ts.filter(t => s.contains(t.covered)).map(t => t.by))
    )
    chain.exclude(Set(c)).intersect(held)
  }

  /// Replies the sender believes: it holds the coverer live and trusts the issuer.
  pure def believable(l: LocalState, replies: Set[Tombstone]): Set[Tombstone] =
    replies.filter(t =>
      t.by != t.covered
        and l.items.exists(i => i.head == t.by and (TRUST == AnyPeer or i.author == t.issuer)))

  /// What a sender does with the tombstones a peer replied with.
  pure def applyReplies(l: LocalState, peer: NodeId, replies: Set[Tombstone]): LocalState = {
    val usable = believable(l, replies)
    val gone = usable.map(t => t.covered)
    match MODE {
      | SuppressPeer => { ...l, suppressed: l.suppressed.union(replies.map(t => (peer, t.covered))) }
      | Adopt => { ...l, items: l.items.filter(i => not(gone.contains(i.head))), tombstones: l.tombstones.union(usable) }
      | Quarantine => {
          ...l,
          items: l.items.filter(i => not(gone.contains(i.head))),
          quarantined: l.quarantined.union(l.items.filter(i => gone.contains(i.head))),
          tombstones: l.tombstones.union(usable),
        }
    }
  }

  /// Items in `sent` the receiver may accept under CONNECTED (all of them otherwise):
  /// grow from those whose boundary is already covered.
  pure def acceptable(held: Set[Item], sent: Set[Item]): Set[Item] =
    if (not(CONNECTED)) sent
    else 1.to(MAX_COMMITS).fold(Set(), (ok, _) =>
      ok.union(sent.filter(i => i.boundary.subseteq(coveredBy(held.union(ok))))))

  /// An honest receiver: refuse what its tombstones say it covers, reply with those
  /// tombstones, store the rest.
  pure def receive(l: LocalState, sent: Set[Item], self: NodeId): { node: LocalState, answer: Set[Tombstone] } = {
    val held = headsOf(l.items)
    val answer = sent.map(i => coverers(l.tombstones, held, i.head).map(c => {
      val stored = l.tombstones.filter(t => t.covered == i.head and t.by == c)
      if (stored.size() > 0) stored else Set({ covered: i.head, by: c, issuer: self })
    }).flatten()).flatten()
    val refused = answer.map(t => t.covered)
    { node: { ...l, items: l.items.union(sent.filter(i => not(refused.contains(i.head)))) }, answer: answer }
  }

  /// What `a` would send `b`: live items `b` doesn't advertise and `a` hasn't been told to suppress.
  pure def sendable(la: LocalState, lb: LocalState, b: NodeId): Set[Item] = {
    val bHeads = headsOf(lb.items)
    val pool = if (SERVE_QUARANTINED) la.items.union(la.quarantined) else la.items
    pool.filter(i => not(bHeads.contains(i.head)) and not(la.suppressed.contains((b, i.head))))
  }

  pure def minimizeNode(g: CommitId -> CommitInfo, l: LocalState, self: NodeId): LocalState = {
    val m = minimize(g, l.items, self)
    { ...l, items: m.items, tombstones: l.tombstones.union(m.tombstones) }
  }

  /// One full, honest push of everything sendable from a to b.
  pure def fullPush(la: LocalState, lb: LocalState, a: NodeId, b: NodeId): { a: LocalState, b: LocalState } = {
    val r = receive(lb, acceptable(lb.items, sendable(la, lb, b)), b)
    { a: applyReplies(la, b, r.answer), b: r.node }
  }

  /// Exchange in both directions and minimize, SETTLE_ROUNDS times.
  pure def settle(g: CommitId -> CommitInfo, la: LocalState, lb: LocalState, a: NodeId, b: NodeId): { a: LocalState, b: LocalState } =
    1.to(SETTLE_ROUNDS).fold({ a: la, b: lb }, (s, _) => {
      val ab = fullPush(s.a, s.b, a, b)
      val ba = fullPush(ab.b, ab.a, b, a)
      { a: minimizeNode(g, ba.b, a), b: minimizeNode(g, ba.a, b) }
    })

  // ── State machine ────────────────────────────────────────────────────────

  def withEver(ns: NodeId -> LocalState): NodeId -> Set[CommitId] =
    NODES.mapBy(n => everCovered.get(n).union(coveredBy(ns.get(n).items)))

  action init: bool = all {
    dag' = Map(),
    nodes' = NODES.mapBy(_ => { items: Set(), quarantined: Set(), tombstones: Set(), suppressed: Set() }),
    everCovered' = NODES.mapBy(_ => Set()),
    oversyncs' = 0,
    replies' = 0,
  }

  /// Write a commit on top of some of the tips `n` covers (an antichain, like real
  /// heads). Enabled only if `n` covers the whole range the new item must span,
  /// since it has to build the blob.
  action write(n: NodeId): bool = {
    val l = nodes.get(n)
    val c = dag.keys().size()
    nondet parents = tipsOf(dag, coveredBy(l.items)).powerset().oneOf()
    nondet d = 0.to(MAX_DEPTH).oneOf()
    val g = dag.put(c, { parents: parents, depth: d })
    val fresh = buildItem(g, c, n)
    val ns = nodes.set(n, { ...l, items: l.items.union(Set(fresh)), tombstones: l.tombstones.union(authorTombstones(fresh)) })
    all {
      c < MAX_COMMITS,
      fresh.content.exclude(Set(c)).subseteq(coveredBy(l.items)),
      not(CONNECTED) or fresh.boundary.subseteq(coveredBy(l.items)),
      dag' = g,
      nodes' = ns,
      everCovered' = withEver(ns),
      oversyncs' = oversyncs,
      replies' = replies,
    }
  }

  /// `a` sends some of the items whose heads `b` doesn't advertise. An honest `b`
  /// refuses items it knows are covered and replies with tombstones; a Byzantine
  /// `b` replies with whatever tombstones it likes.
  action push(a: NodeId, b: NodeId): bool = {
    val la = nodes.get(a)
    val lb = nodes.get(b)
    nondet sent = sendable(la, lb, b).powerset().oneOf()
    nondet forgedFor = sent.powerset().oneOf()
    nondet forgedBy = 0.to(MAX_COMMITS - 1).oneOf()
    val honest = receive(lb, sent, b)
    val answer =
      if (BYZANTINE.contains(b)) forgedFor.map(i => { covered: i.head, by: forgedBy, issuer: b })
      else honest.answer
    val lb2 =
      if (BYZANTINE.contains(b)) { ...lb, items: lb.items.union(sent.filter(i => not(answer.map(t => t.covered).contains(i.head)))) }
      else honest.node
    val ns = nodes.set(b, lb2).set(a, applyReplies(la, b, answer))
    all {
      a != b,
      sent.size() > 0,
      acceptable(lb.items, sent) == sent,
      dag' = dag,
      nodes' = ns,
      everCovered' = withEver(ns),
      oversyncs' = oversyncs + sent.filter(i => i.content.subseteq(coveredBy(lb.items))).size(),
      replies' = replies + answer.size(),
    }
  }

  action minimizeAt(n: NodeId): bool = {
    val l = nodes.get(n)
    val ns = nodes.set(n, minimizeNode(dag, l, n))
    all {
      ns.get(n).items != l.items,
      dag' = dag,
      nodes' = ns,
      everCovered' = withEver(ns),
      oversyncs' = oversyncs,
      replies' = replies,
    }
  }

  action step: bool = {
    nondet n = NODES.oneOf()
    nondet m = NODES.oneOf()
    any {
      write(n),
      push(n, m),
      minimizeAt(n),
    }
  }

  // ── Properties ───────────────────────────────────────────────────────────

  def coveredAt(n: NodeId): Set[CommitId] = coveredBy(nodes.get(n).items)

  def storedAt(n: NodeId): Set[CommitId] = coveredBy(nodes.get(n).items.union(nodes.get(n).quarantined))

  /// No data loss: an honest node keeps the bytes of every commit it once covered.
  val noDataLoss: bool =
    HONEST.forall(n => everCovered.get(n).subseteq(storedAt(n)))

  /// No availability loss: an honest node still serves every commit it once covered.
  val noAvailabilityLoss: bool =
    HONEST.forall(n => everCovered.get(n).subseteq(coveredAt(n)))

  /// Every tombstone an honest node holds is true: covered's range is inside by's.
  val tombstonesTrue: bool =
    HONEST.forall(n => nodes.get(n).tombstones.forall(t =>
      dag.keys().contains(t.by)
        and buildItem(dag, t.covered, n).content.subseteq(buildItem(dag, t.by, n).content)))

  pure def isStranded(g: CommitId -> CommitInfo, items: Set[Item]): bool = {
    val m = minimize(g, items, "").items
    m.exists(i => i.content.subseteq(coveredBy(m.exclude(Set(i)))))
  }

  /// After minimizing, no live item is redundant (its content is covered by the others).
  val noStranded: bool =
    HONEST.forall(n => not(isStranded(dag, nodes.get(n).items)))

  pure def diverges(g: CommitId -> CommitInfo, la: LocalState, lb: LocalState, a: NodeId, b: NodeId): bool = {
    val s = settle(g, la, lb, a, b)
    s.a.items != s.b.items
  }

  /// After two honest nodes settle, each serves everything either ever covered.
  val settledAvailability: bool =
    HONEST.forall(a => HONEST.forall(b => a == b or {
      val s = settle(dag, nodes.get(a), nodes.get(b), a, b)
      val want = everCovered.get(a).union(everCovered.get(b))
      want.subseteq(coveredBy(s.a.items)) and want.subseteq(coveredBy(s.b.items))
    }))

  /// Two honest nodes that exchange everything end up with the same live items.
  val settledConvergence: bool =
    HONEST.forall(a => HONEST.forall(b =>
      a == b or not(diverges(dag, nodes.get(a), nodes.get(b), a, b))))

  // ── Witnesses (reachability) ─────────────────────────────────────────────

  val deepItemWritten: bool =
    NODES.exists(n => nodes.get(n).items.exists(i => depthOf(dag, i.head) > 0 and i.content.size() > 1))

  val minimizeDropped: bool =
    HONEST.exists(n => nodes.get(n).tombstones.size() > 0)

  val oversyncHappened: bool = oversyncs > 0

  val tombstoneReplied: bool = replies > 0

  val quarantineUsed: bool = HONEST.exists(n => nodes.get(n).quarantined.size() > 0)

  // Negated invariants as witnesses: the share of traces that reach a bad state.
  val dataLossReached: bool = not(noDataLoss)
  val availabilityLossReached: bool = not(noAvailabilityLoss)
  val falseTombstoneReached: bool = not(tombstonesTrue)
  val strandedReached: bool = not(noStranded)
  val divergenceReached: bool = not(settledConvergence)
  val unavailableAfterSettle: bool = not(settledAvailability)

  /// Some honest node can't build a deep commit on something it covers.
  val deepWriteBlocked: bool = {
    val c = dag.keys().size()
    HONEST.exists(n => coveredAt(n).exists(p => {
      val g = dag.put(c, { parents: Set(p), depth: MAX_DEPTH })
      not(rangeOf(g, Set(p), MAX_DEPTH).interior.subseteq(coveredAt(n)))
    }))
  }
}

// ── Instances ──────────────────────────────────────────────────────────────

module default {
  import modes.*
  import sedimentree(
    NODES = Set("a", "b", "c"),
    BYZANTINE = Set(),
    MAX_COMMITS = 6,
    MAX_DEPTH = 2,
    MODE = Quarantine,
    WALK = Partial,
    CONNECTED = true,
    SETTLE_ROUNDS = 3,
    TRUST = AuthorOnly,
    SERVE_QUARANTINED = true,
  ).*
}
