#import "@preview/fletcher:0.5.8": diagram, node, edge
#import "@preview/catppuccin:1.1.0": catppuccin, flavors
#import "diagrams.typ": commit-graph, layer-diagram
#import "sedimentree-examples.typ": example-commits, example-fragments, example-order, example-annotations, checkpoint-commits, checkpoint-fragments
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
    A commit graph with four fragments. F3 (depth 3) contains F1 and F2
    (depth 2). F4 is a newer depth 2 fragment. Two loose commits are not
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
    show containment: F1 and F2 share commits, and F3 contains both fragments.
  ],
)

From this diagram we can see that there is some redundancy here which we can
exploit. F3 contains everything in F1 and F2, which in
turn contain a bunch of loose commits. By removing all these redundant entries
we can summarize the tree like this:

#figure(
  layer-diagram(example-commits, example-fragments, order: example-order, minimal: true),
  caption: [
    The same layers with redundant entries removed, without moving the remaining
    items. Commits covered by fragments and fragments F1 and F2 are omitted.
    F3, F4, and the uncovered loose commits `e4c1` and `f6d2` still represent
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

  #v(3mm)
  #text(size: 9pt)[Head-and-boundary metadata available to the relay:]
  #align(center, block(width: 100mm)[
    #set text(size: 9pt)
    #table(
      columns: (1fr, 1fr, 1fr), inset: 4pt, stroke: none,
      [*Fragment*], [*Head*], [*Excluded boundary*],
      table.hline(stroke: 0.5pt + gray),
      ..checkpoint-fragments.map(f => (
        [#f.id], raw(f.head), f.boundaries.map(raw).join([, ]),
      )).flatten(),
    )
  ])
],
  kind: image,
  caption: [
    A and B are contained in G, but this diagram shows membership that the
    relay cannot inspect. Neither A's nor B's head or boundary appears in G's
    head-and-boundary metadata. Even knowing that B stops at A's head does not
    establish that either fragment is inside G. The relay cannot safely omit
    A or B without additional containment information.
  ],
)

In this scenario there is no way to know that `G` actually contains the commits
for `A` and `B` because all we know about `G` is that it starts at `000a` and
ends at `000b`, neither of which are mentioned by the boundaries of `A` or `B`.

To get around this we include in all fragments with $depth > 2$ the boundaries
of all $depth = 2$ commits. We call these checkpoint commits. If we draw the
example above with the checkpoint commits you can see that the fact that `G`
contains `A` and `B` is visible as the boundaries of `A` and `B` are in `G`s
checkpoint commits (all boundary commits are _at least_ $depth = 2$ so any 
fragment contains the boundaries of all commits inside it as checkpoint
commits)



=== Dangling Loose Commits

A relay peer which doesn't have access to the full commit graph can wind up in
a situation where it has a commit (or chain of commits) which it cannot prove
are contained within some deeper fragments. One example of where this can
happen is if some peer is sending commits and then the connection is dropped
whilst the peer continues generating data. The peer eventually bundles all the
commits into a fragment and then reconnects to the relay peer and sends the
fragment. The relay peer now has a fragment which contains the commits it
received earlier but no way to know this.

This is problematic for two reasons. Firstly, the relay peer will oversend
data when syncing with other peers as it cannot prove the commits are not
needed and it will also disagree with clients that _have_ the clear text on
what the heads of the sedimentree are, which means we can end up in a scenario
where the relay continues to send the loose commit to the client.

== Fixups

The solution here is to replace the checkpoint commits with a "fixup"
operation. The simplest version of this idea is that when a client with access
to the cleartext receives a sedimentree summary from another peer it can
send the peer a "fixup" operation describing any redundancies it computed in
the tree the remote advertised.

In the loose commit example above this would mean that the relay peer would
send the sedimentree summary to the client and the client would respond with
a fixup saying "this loose commit is actually contained in this fragment"

=== Propagation

A major problem with fixup operations is that federating relay peers - who do
not load the commit graph - are unable to help each other resolve redundancy.
To solve this we will need to store and propagate fixups between peers. The
most naive version of this would be to store all the fixup operations we have
ever received and forward them all. This will destroy most of the bandwidth
benefits of using a sedimentree in the first place.

Instead we should note that most of the time, once a fixup has been applied it
will not be needed again. Future peers will work with the minimized tree.
Fixups are mostly required for fixing transient redundancy. We do want fixups
to propagate through the network, but it's okay for old ones to be lost once
most of the network has applied the fixup.

=== Fixup Lists

Considering all the above, we define a new part of the  sedimentree data
structure. As well as the fragments and loose commits (which now do not have
checkpoint commits) we also have a fixup list. This is a fixed length list of
fixup operations which describe redundancies in the tree.

Each fixup has, along with the fragments and/or loose commits it applies to,
a number which we call it's "inactivity". This is used to keep track of fixups
which have been used recently.

When we merge two sedimentrees we first merge the trees, then we look through
the fixup list for any fixups which can be applied to the merged tree. For
every fixup which can be applied we set it's inactivity to zero. Finally,
we order the fixup list by inactivity, and then by hash, and discard the tail
of the list if it is longer than the maximum length.

In this manner fixups which are currently useful will be propagated through the
network and then slowly replaced by new fixups as they are needed.
