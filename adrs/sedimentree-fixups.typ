#import "@preview/fletcher:0.5.8": diagram, node, edge
#import "@preview/catppuccin:1.1.0": catppuccin, flavors
#import "diagrams.typ": commit-graph, layer-diagram
#import "sedimentree-examples.typ": example-commits, example-fragments, example-order, example-annotations, checkpoint-commits, checkpoint-fragments
#import "sedimentree-examples.typ": dangling-commits, dangling-fragments
#set document(title: "Sedimentree Fixups")
#set page(paper: "a4")
#set text(size: 11pt)

= Sedimentree Fixups

A modification to sedimentree which replaces checkpoint commits with fixup
operations.

== Introduction

The sedimentree data structure and sync protocol has two related problems
caused by the fact that it is not possible to compute whether one fragment
contains anothe fragment (or loose commit). This document discusses the
problems and proposes that we replace the checkpoint commits which were
intended to solve this problem with a new "fixup" concept.

== Background

Sedimentree splits a document's commit graph into fragments. A fragment includes
its head and walks backwards along parent links, stopping before its boundary
commits, which are excluded. Heads and boundaries have at least $n$ leading
zeros, where $n$ is the fragment's depth.

For example, consider the following commit graph where we have coloured in the
fragment boundaries:


#figure(
  commit-graph(example-commits, fragments: example-fragments, show-fragments: false),
  caption: [
    A commit graph with fragment boundaries coloured. Root is at the top and
    arrows point from child to parent.
  ],
)

This can be split into the following fragments:


#figure(
  commit-graph(example-commits, fragments: example-fragments, annotations: example-annotations),
  caption: [
    A commit graph with five fragments. F5 (depth 3) contains F1, F2, and F3
    (depth 2). F3 and F5 share the head `000f`, but F3 stops before its boundaries
    `00d4` and `00e6`. F4 is a newer depth 2 fragment. Two loose commits are not
    contained in any fragment.
  ],
)

We can think of fragments at a particular depth as a "layer". Deeper fragments
will tend to be earlier in the commit graph, and shallower fragments which are
not contained in deeper fragments will tend to be later in the commit graph.
Hence the name (sedimentry rocks are older the deeper you go).

The commit graph above can be visualized like this:

#figure(
  layer-diagram(example-commits, example-fragments, order: example-order),
  caption: [
    The same graph as a layer diagram, with one row per depth. Aligned spans
    show containment: F1 and F2 share commits, and F5 contains F1, F2, and F3.
  ],
)

From this diagram we can see that there is some redundancy here which we can
exploit. F5 contains everything in F1, F2, and F3, which in
turn contain a bunch of loose commits. By removing all these redundant entries
we can summarize the tree like this:

#figure(
  layer-diagram(example-commits, example-fragments, order: example-order, minimal: true),
  caption: [
    The same layers with redundant entries removed, without moving the remaining
    items. Commits covered by fragments and fragments F1, F2, and F3 are omitted.
    F5, F4, and the uncovered loose commits `e4c1` and `f6d2` still represent
    the entire graph.
  ],
)

In commit graphs which are "deep and narrow" (i.e. have much less concurrency
than history) this summarization can be very efficient. In a completely linear
history with $n$ commits for example, we would see $O(log_16 n)$ fragments and
$O(1)$ loose commits. For 1,000,000 commits this would be $approx 40$ fragments
and $approx 255$ loose commits.

=== Incremental Sync

The goal of this structure is to enable incremental synchronization with peers
that can't access the commit graph. Imagine Bob and Alice are connected to some
relay peer. Initially all three peers have the same document state. Then, Alice
sends a new commit to the relay peer. Bob later begins sync with the relay,
he sends his cover to the relay which realizes that he is missing the new
commit and sends that to him in the response.


#figure({
  set text(size: 9pt)

  // Reuse the same miniature shapes in state snapshots and message labels.
  // Dashed, unfilled shapes denote references rather than available payloads.
  let commit-icon(metadata: false) = circle(
    radius: 2.5mm,
    inset: 0pt,
    stroke: (paint: orange.darken(25%), thickness: 0.6pt,
      dash: if metadata { "dashed" } else { "solid" }),
    fill: if metadata { white } else { orange.lighten(85%) },
    align(center + horizon, text(size: 8pt)[c]),
  )
  let tree(commit: "absent", metadata: false) = grid(
    columns: (14mm, 6mm), rows: (5mm, 5mm),
    column-gutter: 1mm, row-gutter: 1mm,
    [],
    if commit == "absent" { [] } else {
      commit-icon(metadata: metadata)
    },
    rect(width: 14mm, height: 5mm, radius: 1mm, inset: 0pt,
      stroke: (paint: green.darken(30%), thickness: 0.6pt,
        dash: if metadata { "dashed" } else { "solid" }),
      fill: if metadata { white } else { green.lighten(90%) },
      align(center + horizon, text(size: 8pt)[F])),
    [],
  )
  let state(x, y, commit: "absent", note: [Stored]) = node(
    (x, y), align(center, stack(dir: ttb, spacing: 2pt,
      text(size: 8pt, note), tree(commit: commit))),
    shape: rect, width: 27mm, inset: 2mm, corner-radius: 1.5mm,
    stroke: 0.4pt + gray.lighten(50%), fill: white,
  )
  let message-label(x, y, title, picture) = node(
    (x, y), align(center, stack(dir: ttb, spacing: 2pt,
      text(size: 8pt, title), picture)),
    shape: rect, inset: 1pt, stroke: none, fill: white,
  )

  diagram(
    // Physical coordinates: time runs downward (negative y).
    node((0mm, 0mm), [*Alice*], stroke: none),
    node((57mm, 0mm), [*Relay*], stroke: none),
    node((114mm, 0mm), [*Bob*], stroke: none),
    ..(0mm, 57mm, 114mm).map(x => edge(
      (x, -5mm), (x, -141mm),
      stroke: (paint: gray, thickness: 0.5pt, dash: "dashed"),
      snap-to: none, layer: -10,
    )),

    state(0mm, -16mm),
    state(57mm, -16mm),
    state(114mm, -16mm),

    state(0mm, -38mm, commit: "present", note: [Creates c]),
    edge((0mm, -56mm), (57mm, -56mm), "-|>", snap-to: none),
    message-label(28.5mm, -48mm, [1. Commit payload], commit-icon()),
    state(57mm, -68mm, commit: "present"),

    edge((114mm, -87mm), (57mm, -87mm), "-|>", snap-to: none),
    message-label(85.5mm, -77mm, [2. Bob's cover],
      tree(metadata: true)),
    state(57mm, -99mm, commit: "present", note: [Bob lacks c]),

    edge((57mm, -117mm), (114mm, -117mm), "-|>", snap-to: none),
    message-label(85.5mm, -109mm, [3. Commit payload], commit-icon()),
    state(114mm, -130mm, commit: "present", note: [Synchronized]),
  )
},
  caption: [
    Incremental sync between Alice, a relay, and Bob. All three initially store
    fragment F; Alice creates a loose commit c whose parent is in F. Cards on
    the tracks show local state, with a new card after each received message.
    Solid shapes mean payloads are available; dashed shapes mean references
    only. Bob sends his cover containing F; the relay sees that c is missing
    and responds with c's payload. No separate commit request is needed, and
    F's payload is never retransmitted. State persists until the next card
    on a track.
  ],
)

This is the ideal scenario where the cover can precisely identify the missing
commit. In general we will oversend because fragments tend to be redundant,
but this redundancy will tend to be smaller when commit graphs are more 
overlapping - i.e. when we're mostly in sync already we don't have too much
to do. This is good because this is the common case.

=== Checkpoint Commits

There is a wrinkle here though. To perform incremental sync each peer needs to
be able to compute the minimal cover of the document. But in general we don't
have access to the underlying commit graph (the canonical example being e2e
encrypted relays). If we represent a fragment as just it's head + boundaries
then we can wind up in scenarios where we can't calculate this minimal cover.

For example, consider this tree where we have two fragments at depth 2 and one
at depth 4.

#figure([
  #layer-diagram(
    checkpoint-commits, checkpoint-fragments,
    depths: (2, 3, 4), column-spacing: 14mm, commit-width: 13mm,
    commit-text-size: 8pt, highlight-heads: true,
    empty-labels: ("3": [No depth-three fragments]),
  )
],
  kind: image,
  caption: [
    A and B are contained in G, but this diagram shows membership that the
    relay cannot inspect. Neither A's nor B's head or boundary appears in G's
    head or boundary. Even knowing that B stops at A's head does not
    establish that either fragment is inside G. The relay cannot safely omit
    A or B without additional containment information.
  ],
)

In this scenario there is no way to know that `G` actually contains the commits
for `A` and `B` because all we know about `G` is its head `0000b` and excluded
boundary `0000a`, neither of which appears in the metadata for `A` or `B`.

To get around this, fragments with depth $n > 2$ also record internal commits of
at least depth 2 as checkpoint commits. The fragment's own head and boundaries
are already recorded separately. In this example, `G`'s checkpoints identify
both the heads and boundaries of `A` and `B`. If a shallower fragment's boundary
coincides with `G`'s own excluded boundary, we use that boundary metadata rather
than an internal checkpoint.

#figure(
  layer-diagram(
    checkpoint-commits, checkpoint-fragments,
    depths: (2, 3, 4), column-spacing: 14mm, commit-width: 13mm,
    commit-text-size: 8pt, highlight-heads: true, show-checkpoints: true,
    empty-labels: ("3": [No depth-three fragments]),
  ),
  caption: [
    The same layers with G's checkpoint metadata shown as white badges.
    Dotted guides align each badge with its commit column; they are not parent
    links. A's boundary `00a0` and head `00a1`, and B's boundary `00a1` and head
    `00a2`, all appear in G's checkpoints. Fragment positions and horizontal
    spans are unchanged; G's bar is taller to make room for the metadata.
  ],
)<fig:checkpoints>

In @fig:checkpoints we can see that the heads of `A` and `B` are in the
checkpoint commit set and so we know that `A` and `B` are in `G` and can be
discarded from the minimal cover.

== The Problem

There are two problems with checkpoint commits:

- They don't solve the problem of dangling loose commits
- They lead to an $O(n)$ bandwidth growth

=== Dangling Loose Commits

There is an analog to the problem outlined above where we were missing a layer
of fragments. In order to determine if a loose commit can be discarded from the
minimal cover we need a path from that commit to a checkpoint commit.

Imagine we have this commit graph:

#figure(
  commit-graph(dangling-commits, fragments: dangling-fragments),
  caption: [
    Three commits in a line. Fragment F starts at its head `00d5` and walks
    back to the root `a1b2`, including all three commits. Arrows point to parents.
  ],
)<fig:dangling-graph>

Now let's say that on some relay node we have this cover:

#figure(
  layer-diagram(
    dangling-commits, dangling-fragments,
    visible-commits: ("a1b2", "b3c4"),
    column-spacing: 14mm, commit-width: 13mm, commit-text-size: 8pt,
  ),
  caption: [
    A cover of the tree which has `a1b2` and `b3c4` even though they are 
    contained in `F`
  ],
)<fig:dangling-layers>

In this situation the minimal cover is actually `F`, but the relay node has no
way to know this. This scenario can actually happen very easily. This is a
common sequence of events:

- Alice is incrementally syncing a document as she edits it
- Alice creates a commit and sends it
- Alice creates another commit and sends it
- Alice creates a further commit and realises it is a fragment boundary
- Alice creates a new fragment and sends the fragment

Alice doesn't send the final commit in the chain for whatever reason.

#figure({
  set text(size: 9pt)

  let commit-icon(id) = rect(
    width: 9mm, height: 6mm, radius: 1mm, inset: 0pt,
    stroke: 0.6pt,
    fill: if id == "00d5" { green.lighten(85%) } else { white },
    align(center + horizon, text(size: 7.5pt, raw(id))),
  )
  let fragment-icon() = rect(
    width: 29mm, height: 6mm, radius: 1mm, inset: 0pt,
    stroke: 0.6pt + green.darken(30%), fill: green.lighten(90%),
    align(center + horizon)[F],
  )
  // Keep the same three columns in every snapshot. F covers all three,
  // even when the head is not represented as a separate loose commit.
  let cover(loose: (), fragment: false) = {
    if loose.len() == 0 and not fragment {
      text(gray, size: 8pt)[Empty]
    } else {
      stack(dir: ttb, spacing: 1mm,
        grid(columns: (9mm, 9mm, 9mm), rows: (6mm,), column-gutter: 1mm,
          ..("a1b2", "b3c4", "00d5").map(id =>
            if id in loose { commit-icon(id) } else { [] }),
        ),
        if fragment { fragment-icon() } else { [] },
      )
    }
  }
  let state(x, y, note, loose: (), fragment: false) = node(
    (x, y), align(center, stack(dir: ttb, spacing: 2pt,
      text(size: 8pt, note), cover(loose: loose, fragment: fragment))),
    shape: rect, width: 37mm, inset: 2mm, corner-radius: 1.5mm,
    stroke: 0.4pt + gray.lighten(50%), fill: white,
  )
  let message(y, title, picture) = {
    edge((0mm, y), (88mm, y), "-|>", snap-to: none)
    node((44mm, y + 9mm),
      align(center, stack(dir: ttb, spacing: 2pt,
        text(size: 8pt, title), picture)),
      shape: rect, inset: 1pt, stroke: none, fill: white)
  }

  diagram(
    node((0mm, 0mm), [*Alice*], stroke: none),
    node((88mm, 0mm), [*Relay*], stroke: none),
    ..(0mm, 88mm).map(x => edge(
      (x, -5mm), (x, -207mm),
      stroke: (paint: gray, thickness: 0.5pt, dash: "dashed"),
      snap-to: none, layer: -10,
    )),
    state(0mm, -14mm, [Cover]),
    state(88mm, -14mm, [Cover]),

    state(0mm, -36mm, [Creates `a1b2`], loose: ("a1b2",)),
    message(-54mm, [1. Commit payload], commit-icon("a1b2")),
    state(88mm, -68mm, [Cover], loose: ("a1b2",)),

    state(0mm, -82mm, [Creates `b3c4`], loose: ("a1b2", "b3c4")),
    message(-100mm, [2. Commit payload], commit-icon("b3c4")),
    state(88mm, -114mm, [Cover], loose: ("a1b2", "b3c4")),

    state(0mm, -136mm, [Creates `00d5` locally],
      loose: ("a1b2", "b3c4", "00d5")),
    state(0mm, -158mm, [Builds F; cover is now F], fragment: true),
    message(-178mm, [3. Fragment payload], fragment-icon()),
    state(88mm, -194mm, [Redundant cover],
      loose: ("a1b2", "b3c4"), fragment: true),
  )
},
  caption: [
    How dangling loose commits arise. Alice sends `a1b2` and `b3c4` separately,
    then builds and sends F without sending its head `00d5` as a loose commit.
    Alice knows F contains all three commits, so her cover is just F. The relay
    receives F but lacks the commit graph needed to establish containment;
    it retains `a1b2` and `b3c4` alongside F. Cards show the current cover,
    not deletion of stored payloads. Time flows downward.
  ],
)<fig:dangling-sync>

We're now in a situation where the relay will constantly oversend the loose 
commits.

=== $O(n)$ Bandwidth Growth

Including checkpoint commits in fragment metadata means that whilst the number
of fragments required to cover a deep, narrow commit graph is $O(log n)$, the
checkpoint metadata in that cover grows with $O(n)$.

This is particularly problematic for simple LWW CRDTs. Consider a presence CRDT
that publishes each user's last seen mouse position. With 1,000 users updating
once per second, after a day the minimal cover is about 4 MB
of checkpoint data, even though the current state holds only 1,000 positions.
A checkpoint-free, logarithmic cover would instead need only a few hundred
entries in an idealized linear history, roughly 10 kB of identifiers.
#footnote[
  Assuming uniformly distributed hashes, the history contains
  $1000 times 86400 = 86400000$ commits. Two leading hexadecimal zeros select
  roughly one commit in 256, giving 337,500 checkpoints. At 12 bytes each,
  these occupy 4,050,000 bytes (4.05 MB, about 3.86 MiB), or 0.375 bits per
  original commit, before overlap and framing overhead.

  Under the same hexadecimal-depth rule, a linear history of this size has an
  expected minimal cover of approximately 64 fragments and 255 loose commits.
  One 32-byte identifier per entry gives
  $(64 + 255) times 32 = 10208$ bytes (about 10.2 kB). 
]

Simple LWW CRDTs for this kind of data are something we would very much like to
support!


== Fixups

The solution I propose in this document is to add the concept of a "fixup". A 
"fixup" is a small piece of data which says "you can replace fragment `A` with
`B`". The idea here is that when a peer who has the clear text (or is otherwise
willing to materialize the commit graph) is synchronizing with a remote peer
they can detectredundancy in the remote representation of the sedimentree and
inform the remote with a fixup message.
