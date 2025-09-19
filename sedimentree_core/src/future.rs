use futures::future::{BoxFuture, LocalBoxFuture};

pub trait FutureKind {
    type Future<'a, T: 'a>: Future<Output = T> + 'a;
}

#[derive(Debug, Clone, Copy)]
pub enum Sendable {}
impl FutureKind for Sendable {
    type Future<'a, T: 'a> = BoxFuture<'a, T>;
}

#[derive(Debug, Clone, Copy)]
pub enum Local {}
impl FutureKind for Local {
    type Future<'a, T: 'a> = LocalBoxFuture<'a, T>;
}
