// Run: typst compile --root adrs adrs/tests/diagrams.typ /tmp/diagram-tests.pdf
#import "../diagrams.typ": commit-graph, layer-diagram, minimal-cover
#import "../sedimentree-examples.typ": example-commits, example-fragments, example-order, checkpoint-commits, checkpoint-fragments

#set text(size: 11pt)

#let cover = minimal-cover(example-commits, example-fragments)
#assert.eq(cover.commits, ("e4c1", "f6d2"))
#assert.eq(cover.fragments, ("F3", "F4"))

// Boundaries are excluded: G does not cover 0000a.
#let checkpoint-cover = minimal-cover(checkpoint-commits, checkpoint-fragments)
#assert.eq(checkpoint-cover.commits, ("0000a",))
#assert.eq(checkpoint-cover.fragments, ("G",))

// Equal membership keeps one representative rather than removing both.
#let first = example-fragments.first()
#let duplicate = first + (id: "duplicate")
#let duplicate-cover = minimal-cover(example-commits, (first, duplicate))
#assert.eq(duplicate-cover.fragments, ("F1",))

#assert.eq(minimal-cover((), ()), (commits: (), fragments: ()))
#assert.eq(minimal-cover(example-commits, ()).commits, example-commits.map(c => c.id))

// The renderer must preserve bounds even when ALL commit boxes disappear.
#let covered-commits = ((id: "a"), (id: "b"))
#let covering-fragment = (
  (id: "F", depth: 2, head: "b", boundaries: (), members: ("a", "b"), color: blue),
)
#assert.eq(minimal-cover(covered-commits, covering-fragment).commits, ())

#context {
  let same-bounds(commits, fragments, order: auto) = {
    let full = measure(layer-diagram(commits, fragments, order: order))
    let reduced = measure(layer-diagram(commits, fragments, order: order, minimal: true))
    assert(calc.abs(full.width - reduced.width) < 0.01pt, message: "Minimization changed width")
    assert(calc.abs(full.height - reduced.height) < 0.01pt, message: "Minimization changed height")
  }
  same-bounds(example-commits, example-fragments, order: example-order)
  same-bounds(checkpoint-commits, checkpoint-fragments)
  same-bounds(covered-commits, covering-fragment)
  same-bounds((), ())
}

Diagram checks passed.

// Smoke-test the generic graph renderer independently of the ADR examples.
#commit-graph(
  ((id: "a", pos: (0mm, 0mm)),
   (id: "b", pos: (0mm, -15mm), parents: ("a",))),
  fragments: covering-fragment,
)
