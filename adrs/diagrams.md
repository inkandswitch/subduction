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
- `column-spacing`, `commit-width`, and `commit-text-size` control layer sizing.
  `highlight-heads: true` colours heads in layer diagrams too.
- Optional per-fragment styling and graph layout fields are documented at the
  top of `diagrams.typ`; the existing examples show their use.

## Checking changes

From the repository root:

```sh
typst compile adrs/sedimentree-fixups.typ /tmp/sedimentree-fixups.pdf
typst compile --root adrs adrs/tests/diagrams.typ /tmp/diagram-tests.pdf
```

The checks cover minimization, excluded boundaries, equal-member fragments,
empty inputs, generic graph rendering, and matching full/minimal layout bounds.
