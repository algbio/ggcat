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
unsafe impl Send for ExecutorAddress {}
unsafe impl Sync for ExecutorAddress {}

impl ExecutorAddress {
    pub fn executor_allocated(&self) -> bool {
        self.executor_keeper.read().is_some()
    }

    pub fn to_weak(&self) -> WeakExecutorAddress {
        WeakExecutorAddress {
            executor_keeper: self
                .executor_keeper
                .read()
                .as_ref()
                .map(|x| Arc::downgrade(x)),
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
    executor_keeper: Option<Weak<dyn Any>>,
    executor_type_id: u64,
    executor_internal_id: u64,
}
unsafe impl Send for WeakExecutorAddress {}
unsafe impl Sync for WeakExecutorAddress {}

impl WeakExecutorAddress {
    pub(crate) fn empty() -> Self {
        Self {
            executor_keeper: None,
            executor_type_id: 0,
            executor_internal_id: 0,
        }
    }

    pub(crate) fn get_strong(&self) -> Option<ExecutorAddress> {
        let strong_ref = self.executor_keeper.as_ref()?.upgrade()?;
        Some(ExecutorAddress {
            executor_keeper: Arc::new(RwLock::new(Some(strong_ref))),
            executor_type_id: self.executor_type_id,
            executor_internal_id: self.executor_internal_id,
        })
    }
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
