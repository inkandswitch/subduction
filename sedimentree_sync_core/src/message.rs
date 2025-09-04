use sedimentree_core::{Sedimentree, SedimentreeId};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Message {
    Want(Want),
    Have(Have),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Want {
    pub id: SedimentreeId,
}

impl From<Want> for Message {
    fn from(want: Want) -> Message {
        Message::Want(want)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Have {
    pub sedimentree: Sedimentree,
}

impl From<Have> for Message {
    fn from(have: Have) -> Message {
        Message::Have(have)
    }
}
