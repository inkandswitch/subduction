// Reusable sedimentree illustrations, independent of any particular example.
#import "@preview/fletcher:0.5.8": diagram, node, edge, hide

// Data format:
// - Commit: (id: "hash", pos: (x, y), parents: ("parent", ...)).
//   pos and parents are only needed for commit-graph. A parent may instead be
//   (id: "hash", bend: -45deg). Optional fill / overlay-fill override colours.
// - Fragment: (id: "F1", depth: 2, head: "hash", boundaries: ("hash", ...),
//   members: ("hash", ...), color: blue).
//   members includes the head but EXCLUDES boundaries. Membership is supplied
//   by the author: these helpers do not infer it from head/boundary metadata.
//   Optional styling: stroke, bar-fill, bar-align; graph-specific overrides:
//   graph-inset, graph-stroke, graph-fill, graph-layer, graph-label-pos,
//   graph-label-color.
// - Coordinates are physical lengths; positive y points upward.

#let validate(commits, fragments) = {
  let ids = commits.map(c => c.id)
  assert(ids.dedup().len() == ids.len(), message: "Commit IDs must be unique")
  let names = fragments.map(f => f.id)
  assert(names.dedup().len() == names.len(), message: "Fragment IDs must be unique")
  for f in fragments {
    assert(f.depth > 0, message: "Fragment depths must be positive")
    assert(f.head in f.members, message: "A fragment must include its head")
    assert(f.members.dedup().len() == f.members.len(), message: "Duplicate fragment member")
    assert(f.members.all(id => id in ids), message: "Unknown fragment member")
    assert(f.at("boundaries", default: ()).all(id => id not in f.members),
      message: "A fragment must exclude its boundaries")
  }
}

#let fragment-stroke(f) = f.at("stroke", default: f.color)

#let commit-fill(c, fragments, highlight-heads: false, overlays: false) = {
  if overlays and "overlay-fill" in c { return c.overlay-fill }
  if "fill" in c { return c.fill }
  if highlight-heads {
    let heads = fragments.filter(f => f.head == c.id)
    if heads.len() > 0 { return heads.first().color.lighten(85%) }
  }
  white
}

// Draw a graph from parent links, optionally overlaying fragment membership.
// Head highlights remain visible when show-fragments is false. Annotation
// records (pos, body) are shown only with overlays, like fragment labels.
#let commit-graph(commits, fragments: (), show-fragments: true, annotations: ()) = {
  validate(commits, fragments)
  let ids = commits.map(c => c.id)
  diagram(node-stroke: 0.7pt, node-fill: white, node-shape: circle, {
    for c in commits {
      node(c.pos, raw(c.id), name: label(c.id), radius: 4.5mm,
        fill: commit-fill(c, fragments, highlight-heads: true, overlays: show-fragments))
    }
    if show-fragments {
      for f in fragments {
        node(
          enclose: f.members.map(label),
          shape: rect, corner-radius: 3mm,
          inset: f.at("graph-inset", default: 3mm),
          stroke: f.at("graph-stroke", default: fragment-stroke(f)),
          fill: f.at("graph-fill", default: f.color.transparentize(94%)),
          layer: f.at("graph-layer", default: -1),
        )
        if "graph-label-pos" in f {
          node(f.graph-label-pos, text(f.at("graph-label-color", default: f.color), size: 9pt)[#f.id ($n = #f.depth$)],
            shape: rect, stroke: none, fill: none)
        }
      }
      for annotation in annotations {
        node(annotation.pos, text(size: 9pt, annotation.body),
          shape: rect, stroke: none, fill: none)
      }
    }
    for c in commits {
      for parent in c.at("parents", default: ()) {
        let p = if type(parent) == str { (id: parent) } else { parent }
        assert(p.id in ids, message: "Unknown parent commit")
        edge(label(c.id), label(p.id), "-|>", bend: p.at("bend", default: 0deg))
      }
    }
  })
}

// Ground-truth minimization for illustrations, NOT a relay-side algorithm.
// Retain maximal fragments and commits outside all fragments. Equal member
// sets keep the first fragment, so duplicates cannot remove each other.
#let minimal-cover(commits, fragments) = {
  validate(commits, fragments)
  (
    commits: commits.filter(c => not fragments.any(f => c.id in f.members)).map(c => c.id),
    fragments: fragments.enumerate().filter(((i, f)) => {
      not fragments.enumerate().any(((j, other)) => {
        (i != j and f.members.all(id => id in other.members)
          and (other.members.len() > f.members.len() or j < i))
      })
    }).map(((i, f)) => f.id),
  )
}

// Draw containment-aligned layers. order lists every commit ID exactly once;
// each fragment's members must occupy a contiguous span of these columns.
// Explicit depths can include empty layers, with optional empty-labels keyed
// by depth string (e.g. ("3": [No depth-three fragments])).
// Hidden redundant entries still contribute to layout bounds, so the full
// and minimal views have IDENTICAL coordinates, size, and centering.
#let layer-diagram(
  commits, fragments,
  order: auto, depths: auto, minimal: false,
  column-spacing: 10mm, commit-width: 9mm, commit-text-size: 7.5pt,
  highlight-heads: false, empty-labels: (:),
) = {
  validate(commits, fragments)
  let ids = commits.map(c => c.id)
  let order = if order == auto { ids } else { order }
  let depths = if depths == auto { fragments.map(f => f.depth).dedup().sorted() } else { depths }
  assert(order.sorted() == ids.sorted(), message: "Column order must contain every commit exactly once")
  assert(depths.dedup().len() == depths.len(), message: "Layer depths must be unique")
  assert(fragments.all(f => f.depth in depths), message: "Missing fragment depth in layers")
  let cover = if minimal { minimal-cover(commits, fragments) } else { (commits: ids, fragments: fragments.map(f => f.id)) }
  let column-x(i) = 30mm + i * column-spacing
  let row-y(depth) = -(depths.position(d => d == depth) + 1) * 20mm

  diagram(node-stroke: 0.7pt, node-fill: white, {
    node((0mm, 0mm), align(right)[Commits], width: 24mm, stroke: none)
    for depth in depths {
      node((0mm, row-y(depth)), align(right)[Depth #depth], width: 24mm, stroke: none)
    }
    for (i, id) in order.enumerate() {
      let c = commits.find(c => c.id == id)
      let item = node((column-x(i), 0mm), text(size: commit-text-size, raw(id)),
        shape: rect, width: commit-width, height: 10mm,
        inset: 0.5mm, corner-radius: 1mm,
        fill: commit-fill(c, fragments, highlight-heads: highlight-heads))
      if id in cover.commits { item } else { hide(item) }
    }
    for f in fragments {
      let columns = order.enumerate().filter(((i, id)) => id in f.members).map(((i, id)) => i)
      let first = columns.first()
      let last = columns.last()
      assert(last - first + 1 == columns.len(),
        message: "Fragment " + f.id + " is not contiguous: reorder its commit columns")
      let item = node(((column-x(first) + column-x(last)) / 2, row-y(f.depth)),
        align(f.at("bar-align", default: center), [#f.id]),
        shape: rect, width: (last - first) * column-spacing + commit-width,
        height: 12mm, inset: 3mm, corner-radius: 2mm,
        stroke: fragment-stroke(f), fill: f.at("bar-fill", default: f.color.lighten(90%)))
      if f.id in cover.fragments { item } else { hide(item) }
    }
    for depth in depths {
      if not fragments.any(f => f.depth == depth) and str(depth) in empty-labels {
        let center-x = if fragments.len() > 0 {
          let columns = order.enumerate().filter(((i, id)) => fragments.any(f => id in f.members)).map(((i, id)) => i)
          (column-x(columns.first()) + column-x(columns.last())) / 2
        } else { (column-x(0) + column-x(calc.max(0, order.len() - 1))) / 2 }
        node((center-x, row-y(depth)), text(gray, size: 9pt, empty-labels.at(str(depth))),
          shape: rect, stroke: none)
      }
    }
  })
}
