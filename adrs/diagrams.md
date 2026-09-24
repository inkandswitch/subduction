# Sedimentree diagrams

`diagrams.typ` contains reusable renderers. `sedimentree-examples.typ` contains
only example data. The ADR keeps the prose, captions, and figure placement.

## Reusing an example

```typst
#import "diagrams.typ": commit-graph, layer-diagram
#import "sedimentree-examples.typ": example-commits, example-fragments, example-order

// Graph without overlays; fragment heads still retain their colours.
#commit-graph(example-commits, fragments: example-fragments, show-fragments: false)

// The same graph with fragment outlines.
#commit-graph(example-commits, fragments: example-fragments)

// All entries, then the minimal cover. Remaining items do not move.
#layer-diagram(example-commits, example-fragments, order: example-order)
#layer-diagram(example-commits, example-fragments, order: example-order, minimal: true)
```

## Defining a new example

```typst
#let commits = (
  (id: "abcd", pos: (0mm, 0mm)),
  (id: "00ef", pos: (0mm, -15mm), parents: ("abcd",)),
)
#let fragments = (
  (
    id: "F", depth: 2, head: "00ef", boundaries: (),
    members: ("abcd", "00ef"), color: blue,
    graph-label-pos: (20mm, -7mm),
  ),
)

#commit-graph(commits, fragments: fragments)
#layer-diagram(commits, fragments)
```

- Commit IDs also label the nodes. Graphs require `pos`; positive y points up.
  `parents` defaults to empty. A curved parent edge can use
  `(id: "abcd", bend: -45deg)` instead of a string.
- Fragment `members` include the head but **exclude boundaries**. A root reached
  by a walk belongs in `members` regardless of its hash.
- Graph outlines enclose `members`. Layer bars derive their positions and widths
  from the same membership, so the two views cannot silently disagree.
- Layer `order` defaults to commit-array order. Every fragment must occupy a
  contiguous span of columns; the renderer rejects orders that would imply
  incorrect containment. Reorder the columns to put shared members between
  overlapping fragments.
- Use `depths: (2, 3, 4)` to include an empty layer. An optional label can be set
  with `empty-labels: ("3": [No depth-three fragments])`.
- `minimal: true` hides covered commits and contained fragments while retaining
  their layout bounds. **This uses author-supplied membership**, not an algorithm
  available to a relay with only head-and-boundary metadata.
- `visible-commits: ("a1b2", "b3c4")` restricts the top row to specific loose
  entries, for example in a partially redundant relay state. Hidden commits
  keep their columns and still contribute to fragment membership and bounds.
  The default `auto` shows all commits; `()` hides all top-row commits. If used
  with `minimal: true`, only entries surviving both filters are shown.
- `column-spacing`, `commit-width`, and `commit-text-size` control layer sizing.
  `highlight-heads: true` colours heads in layer diagrams too.
- Optional per-fragment styling and graph layout fields are documented at the
  top of `diagrams.typ`; the existing examples show their use.

## Showing checkpoint metadata

Add `checkpoints: ("00a0", "00a1", "00a2")` to a fragment's data, then enable
`show-checkpoints: true` on `layer-diagram`. For example:

```typst
#import "sedimentree-examples.typ": checkpoint-commits, checkpoint-fragments

#layer-diagram(checkpoint-commits, checkpoint-fragments,
  depths: (2, 3, 4), column-spacing: 14mm, commit-width: 13mm,
  show-checkpoints: true)
```

Checkpoints appear as white metadata badges inside their fragment bar, directly
below their commit columns. Dotted guides help match the hashes; disable them
with `checkpoint-guides: false`. Bars with checkpoints grow from 12mm to 18mm
high, but their centers, horizontal spans, and all other positions stay fixed.

Checkpoint IDs must be unique included members, not excluded boundaries. A
fragment's own excluded boundaries remain separate metadata. Badges are not
extra commits or fragments and do not affect minimization. Hiding a redundant
fragment also hides its badges and guides without changing layout bounds.

## Checking changes

From the repository root:

```sh
typst compile adrs/sedimentree-fixups.typ /tmp/sedimentree-fixups.pdf
typst compile --root adrs adrs/tests/diagrams.typ /tmp/diagram-tests.pdf
```

The checks cover minimization, excluded boundaries, equal-member fragments,
empty inputs, generic graph rendering, checkpoint annotations, and matching
full/minimal layout bounds with and without checkpoints, plus selective
visibility of loose commits.
