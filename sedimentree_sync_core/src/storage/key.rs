use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StorageKey(Vec<String>);

impl StorageKey {
    pub fn new(key: Vec<String>) -> Self {
        Self(key)
    }

    pub fn as_slice(&self) -> &[String] {
        self.0.as_slice()
    }

    pub fn to_vec(&self) -> Vec<String> {
        self.0.clone()
    }

    pub fn into_vec(self) -> Vec<String> {
        self.0
    }
}

impl From<Vec<String>> for StorageKey {
    fn from(key: Vec<String>) -> Self {
        Self::new(key)
    }
}
