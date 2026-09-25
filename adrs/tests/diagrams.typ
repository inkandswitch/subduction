// Run: typst compile --root adrs adrs/tests/diagrams.typ /tmp/diagram-tests.pdf
#import "../diagrams.typ": commit-graph, layer-diagram, minimal-cover, commit-fill
#import "../sedimentree-examples.typ": example-commits, example-fragments, example-order, checkpoint-commits, checkpoint-fragments

#import "../sedimentree-examples.typ": dangling-commits, dangling-fragments

#set text(size: 11pt)

#let cover = minimal-cover(example-commits, example-fragments)
#assert.eq(cover.commits, ("e4c1", "f6d2"))
#assert.eq(cover.fragments, ("F5", "F4"))

// The same head has both a shallow fragment and a deeper containing fragment.
#let f3 = example-fragments.find(f => f.id == "F3")
#let f5 = example-fragments.find(f => f.id == "F5")
#assert.eq(f3.depth, 2)
#assert.eq(f5.depth, 3)
#assert.eq(f3.head, f5.head)
#assert.eq(f3.members, ("c9b2", "000f"))
#assert.eq(f3.boundaries, ("00d4", "00e6"))
#assert(f3.boundaries.all(id => id not in f3.members))
#assert(f3.members.all(id => id in f5.members))
#let shared-head = example-commits.find(c => c.id == "000f")
#assert.eq(commit-fill(shared-head, example-fragments, highlight-heads: true),
  f5.color.lighten(85%))
#assert.eq(commit-fill(shared-head, example-fragments.rev(), highlight-heads: true),
  f5.color.lighten(85%))
#assert.eq(minimal-cover(dangling-commits, dangling-fragments),
  (commits: (), fragments: ("F",)))

// Boundaries are excluded: G does not cover 0000a.
#let checkpoint-cover = minimal-cover(checkpoint-commits, checkpoint-fragments)
#assert.eq(checkpoint-cover.commits, ("0000a",))
#assert.eq(checkpoint-cover.fragments, ("G",))
#assert.eq(checkpoint-fragments.last().checkpoints, ("00a0", "00a1", "00a2"))
// Checkpoint annotations must not change ground-truth minimization.
#assert.eq(checkpoint-cover, minimal-cover(checkpoint-commits,
  checkpoint-fragments.map(f => f + (checkpoints: ()))))

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
  let same-bounds(commits, fragments, order: auto, show-checkpoints: false) = {
    let full = measure(layer-diagram(commits, fragments, order: order, show-checkpoints: show-checkpoints))
    let reduced = measure(layer-diagram(commits, fragments, order: order, minimal: true, show-checkpoints: show-checkpoints))
    assert(calc.abs(full.width - reduced.width) < 0.01pt, message: "Minimization changed width")
    assert(calc.abs(full.height - reduced.height) < 0.01pt, message: "Minimization changed height")
  }
  same-bounds(example-commits, example-fragments, order: example-order)
  same-bounds(checkpoint-commits, checkpoint-fragments)
  same-bounds(covered-commits, covering-fragment)
  same-bounds((), ())
  same-bounds(checkpoint-commits, checkpoint-fragments, show-checkpoints: true)
  // Hidden fragments must hide their badges and guides as a group, but keep bounds.
  let annotated = example-fragments.enumerate().map(((i, f)) =>
    if i == 0 { f + (checkpoints: ("00d4",)) } else { f })
  same-bounds(example-commits, annotated, order: example-order, show-checkpoints: true)

  let plain = measure(layer-diagram(checkpoint-commits, checkpoint-fragments))
  let badges = measure(layer-diagram(checkpoint-commits, checkpoint-fragments, show-checkpoints: true))
  let no-guides = measure(layer-diagram(checkpoint-commits, checkpoint-fragments,
    show-checkpoints: true, checkpoint-guides: false))
  assert(calc.abs(plain.width - badges.width) < 0.01pt, message: "Checkpoints changed column span")
  assert.eq(badges, no-guides)
  // Selective top-row visibility preserves the head's column and all bounds.
  let all-commits = measure(layer-diagram(dangling-commits, dangling-fragments))
  let loose-only = measure(layer-diagram(dangling-commits, dangling-fragments,
    visible-commits: ("a1b2", "b3c4")))
  let fragment-only = measure(layer-diagram(dangling-commits, dangling-fragments,
    visible-commits: ()))
  assert.eq(all-commits, loose-only)
  assert.eq(all-commits, fragment-only)
  // Turning the option on without checkpoint data is a no-op.
  assert.eq(measure(layer-diagram(covered-commits, covering-fragment)),
    measure(layer-diagram(covered-commits, covering-fragment, show-checkpoints: true)))
}

Diagram checks passed.

// Smoke-test the generic graph renderer independently of the ADR examples.
#commit-graph(
  ((id: "a", pos: (0mm, 0mm)),
   (id: "b", pos: (0mm, -15mm), parents: ("a",))),
  fragments: covering-fragment,
)

// Smoke-test the badges and optional guides with the actual checkpoint example.
#layer-diagram(checkpoint-commits, checkpoint-fragments,
  depths: (2, 3, 4), column-spacing: 14mm, commit-width: 13mm,
  show-checkpoints: true)
#layer-diagram(checkpoint-commits, checkpoint-fragments,
  depths: (2, 3, 4), column-spacing: 14mm, commit-width: 13mm,
  show-checkpoints: true, checkpoint-guides: false, minimal: true)

#commit-graph(dangling-commits, fragments: dangling-fragments)
#layer-diagram(dangling-commits, dangling-fragments,
  visible-commits: ("a1b2", "b3c4"))
