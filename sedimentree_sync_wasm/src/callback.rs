use js_sys::Function;
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Callback {
    function: Function,
    source_digest: u64,
}

impl From<Function> for Callback {
    fn from(function: Function) -> Self {
        let fn_str: String = function.to_string().into();
        web_sys::console::log_1(&format!("Creating Callback from function: {}", fn_str).into());

        let mut hasher = DefaultHasher::new();
        fn_str.hash(&mut hasher);
        let source_digest = hasher.finish();

        Self {
            function,
            source_digest,
        }
    }
}

impl Hash for Callback {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.source_digest.hash(state);
    }
}
