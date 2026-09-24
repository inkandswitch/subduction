// Example data shared by the commit graphs, layer diagrams, and metadata table.
// Fragment membership is explicit and excludes each fragment's boundaries.

#let example-commits = (
  (id: "a1b3", pos: (0mm, 0mm), overlay-fill: gray.lighten(85%)),
  (id: "a8e5", pos: (0mm, -12mm), parents: ("a1b3",)),
  (id: "b7c2", pos: (-22mm, -24mm), parents: ("a8e5",)),
  (id: "f391", pos: (22mm, -24mm), parents: ("a8e5",)),
  (id: "00d4", pos: (-22mm, -36mm), parents: ("b7c2",)),
  (id: "00e6", pos: (22mm, -36mm), parents: ("f391",)),
  (id: "c9b2", pos: (0mm, -49.5mm), parents: ("00d4", "00e6")),
  (id: "000f", pos: (0mm, -64mm), parents: ("c9b2",)),
  (id: "d8a2", pos: (0mm, -86mm), parents: ("000f",)),
  (id: "00b9", pos: (0mm, -100mm), parents: ("d8a2",)),
  (id: "e4c1", pos: (-16mm, -116mm), parents: ("00b9",)),
  (id: "f6d2", pos: (16mm, -116mm), parents: ((id: "c9b2", bend: -45deg),)),
)

#let example-fragments = (
  (
    id: "F1", depth: 2, head: "00d4", boundaries: (),
    members: ("a1b3", "a8e5", "b7c2", "00d4"),
    color: blue, bar-fill: blue.transparentize(90%), bar-align: left,
    graph-inset: 3mm, graph-layer: -2, graph-label-pos: (-19mm, -6mm),
    graph-stroke: (paint: blue, thickness: 1pt),
  ),
  (
    id: "F2", depth: 2, head: "00e6", boundaries: (),
    members: ("a1b3", "a8e5", "f391", "00e6"),
    color: orange, bar-fill: orange.transparentize(90%), bar-align: right,
    stroke: (paint: orange.darken(25%), thickness: 1pt, dash: "dashed"),
    graph-inset: 4mm, graph-label-pos: (20mm, -6mm),
    graph-label-color: orange.darken(25%),
  ),
  (
    id: "F3", depth: 3, head: "000f", boundaries: (),
    members: ("a1b3", "a8e5", "b7c2", "f391", "00d4", "00e6", "c9b2", "000f"),
    color: green, stroke: green.darken(30%),
    graph-inset: 9mm, graph-layer: -3, graph-label-pos: (-59mm, -60mm),
    graph-stroke: (paint: green.darken(30%), thickness: 1.2pt),
    graph-fill: green.transparentize(96%), graph-label-color: green.darken(30%),
  ),
  (
    id: "F4", depth: 2, head: "00b9", boundaries: ("000f",),
    members: ("d8a2", "00b9"),
    color: purple, stroke: (paint: purple, thickness: 1pt, dash: "dashed"),
    graph-label-pos: (28mm, -92mm),
  ),
)

// F1-only | shared by F1/F2 | F2-only | F3-only | F4 | loose.
// Every fragment occupies a contiguous span in this order.
#let example-order = (
  "b7c2", "00d4", "a1b3", "a8e5", "f391", "00e6",
  "c9b2", "000f", "d8a2", "00b9", "e4c1", "f6d2",
)

#let example-annotations = (
  (pos: (48mm, -116mm), body: [Loose commits]),
)

// Independent checkpoint example. The columns follow a hidden linear history;
// these commits need no graph coordinates because only layers are drawn.
#let checkpoint-commits = (
  (id: "0000a", fill: gray.lighten(90%)),
  (id: "00a0"), (id: "a8e5"), (id: "00a1"),
  (id: "b7c2"), (id: "00a2"), (id: "c9b2"), (id: "0000b"),
)

#let checkpoint-fragments = (
  (
    id: "A", depth: 2, head: "00a1", boundaries: ("00a0",),
    members: ("a8e5", "00a1"), color: blue,
  ),
  (
    id: "B", depth: 2, head: "00a2", boundaries: ("00a1",),
    members: ("b7c2", "00a2"), color: orange,
    stroke: (paint: orange.darken(25%), thickness: 1pt, dash: "dashed"),
  ),
  (
    id: "G", depth: 4, head: "0000b", boundaries: ("0000a",),
    members: ("00a0", "a8e5", "00a1", "b7c2", "00a2", "c9b2", "0000b"),
    // Internal depth-two heads/boundaries. G's own head and excluded boundary
    // are already carried separately in its metadata.
    checkpoints: ("00a0", "00a1", "00a2"),
    color: green, stroke: green.darken(30%),
  ),
)

// The relay received the first two commits individually before receiving F.
// Neither loose commit qualifies as a checkpoint, though both belong to F.
#let dangling-commits = (
  (id: "a1b2", pos: (0mm, 0mm)),
  (id: "b3c4", pos: (0mm, -15mm), parents: ("a1b2",)),
  (id: "00d5", pos: (0mm, -30mm), parents: ("b3c4",)),
)

#let dangling-fragments = (
  (
    id: "F", depth: 2, head: "00d5", boundaries: (),
    members: ("a1b2", "b3c4", "00d5"), color: green,
    stroke: green.darken(30%), graph-label-color: green.darken(30%),
    graph-label-pos: (-24mm, -15mm),
  ),
)
