# Sedimentree Fixups

A modification to sedimentree which replaces checkpoint commits with fixup 
operations.

## Motivation

An important part of what makes sedimentree useful is that the minimal set of
fragments and loose commits which contains a commit graph is logairthmic in the
number of commits in the graph. Unfortunately, in order to compute this minimal
tree we have to store one in every ~256 commit hashes of the entire history of
the document on disk. This leads to an O(n) growth in the storage of a document
- prohibiting use for very fast moving data (such as the mouse movements of a
use in a collaborative drawing application).

Separately, the computation of the minimal tree is imprecise, which can lead
to convergence failures between peers running over the sedimentree encoding of
the commit graph and peers running over the more detailed full commit graph.

### Checkpoint Commits

A sedimentree is an encoding of a commit graph as an increasingly large set of 
"fragments". Each fragment is a region of the commit graph defined by one
head commit and a number of boundary commits. The commits in the region are
found by starting at the head and and traversing backwards (following parent
links) until all of the boundary commits have been reached.

Fragments are defined by the number of leading zero characters in the hex 
encoding of the commit hashes. A depth 2 fragment for example is defined by 
a head commit that starts with two leading zeros and all of the ancestor
commits that have two leading zeros and are reachable without passing through
a commit that has two leading zeros.

This definition of a fragment means that peers who have overlapping sets of 
commits will agree on fragments in the overlapping region. This combined with
the fact that the depth of a fragment is determined by hash hardness means that
over time the "older" regions of the commit graph will be encoded into larger
and larger fragments.

When synchronizing with another peer we send them the "minimal" set of
fragments and loose commits which we think is required to contain the entire
commit graph. This minimal set is computed by starting with the deepest
fragments then filtering out any fragments that are contained in those deepest
fragments, then ascending a depth and repeating the process until we reach
loose commits.

In order for this logic to work we need to be able to compute the containment
relationship between two fragments. In order to do _this_ we need to be able
to compare two fragments at different depths and determine if the lower depth
contains the shallower depth. Just having the head and boundary commits of the
respective fragments is not enough information. To solve this problem we also
encode in each fragment the "checkpoint" commits - these are the hashes of all
the lowest but one (typically the shallowest depth we use is depth 2) commits
in a given fragment. With this information we are able to compare two fragments
at different depths and compute containment by checking if the head and
boundaries of the shallower fragment are contained in the checkpoint commits of
the deeper fragment.

This works well - but it requires shipping around the checkpoint commits. We
truncate these to 12 bits but still, we have quite a bit of extra data.

### Dangling Loose Commits

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

## Fixups

The solution here is to replace the checkpoint commits with a "fixup" 
operation. The simplest version of this idea is that when a client with access
to the cleartext receives a sedimentree summary from another peer it can 
send the peer a "fixup" operation describing any redundancies it computed in
the tree the remote advertised.

In the loose commit example above this would mean that the relay peer would
send the sedimentree summary to the client and the client would respond with
a fixup saying "this loose commit is actually contained in this fragment"

### Propagation

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

### Fixup Lists

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






