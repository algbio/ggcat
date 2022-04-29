use parking_lot::RwLock;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub struct ExecutorAddress {
    pub(crate) executor_keeper: Arc<RwLock<Option<Arc<dyn Any>>>>,
    pub(crate) executor_type_id: u64,
    pub(crate) executor_internal_id: u64,
}

impl ExecutorAddress {
    pub fn executor_allocated(&self) -> bool {
        self.executor_keeper.read().is_some()
    }

    pub fn to_weak(&self) -> WeakExecutorAddress {
        WeakExecutorAddress {
            executor_type_id: self.executor_type_id,
            executor_internal_id: self.executor_internal_id,
        }
    }
}

impl Hash for ExecutorAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.executor_type_id);
        state.write_u64(self.executor_internal_id);
    }
}
impl PartialEq for ExecutorAddress {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        (self.executor_type_id, self.executor_internal_id)
            .eq(&(other.executor_type_id, other.executor_internal_id))
    }
}
impl Eq for ExecutorAddress {}

#[derive(Clone)]
pub struct WeakExecutorAddress {
    executor_type_id: u64,
    executor_internal_id: u64,
}

impl Hash for WeakExecutorAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.executor_type_id);
        state.write_u64(self.executor_internal_id);
    }
}
impl PartialEq for WeakExecutorAddress {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        (self.executor_type_id, self.executor_internal_id)
            .eq(&(other.executor_type_id, other.executor_internal_id))
    }
}
impl Eq for WeakExecutorAddress {}
