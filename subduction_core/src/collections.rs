//! Concurrent collection types.
//!
//! - [`ShardedMap`](sharded_map::ShardedMap): a lock-sharded concurrent map.
//! - [`BoundedShardedMap`](bounded_sharded_map::BoundedShardedMap): a
//!   capacity-bounded LRU cache built on top of a
//!   [`ShardedMap`](sharded_map::ShardedMap).

pub mod bounded_sharded_map;
pub mod sharded_map;
