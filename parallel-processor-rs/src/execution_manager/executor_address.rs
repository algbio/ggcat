use crate::execution_manager::manager::{ExecutionManagerTrait, GenericExecutor};
use parking_lot::RwLock;
use std::any::{Any, TypeId};
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub struct ExecutorAddress {
    pub(crate) executor_keeper: Arc<RwLock<Option<GenericExecutor>>>,
    pub(crate) executor_type_id: TypeId,
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
        self.executor_type_id.hash(state);
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
    pub executor_keeper: Option<Weak<dyn ExecutionManagerTrait>>,
    executor_type_id: TypeId,
    executor_internal_id: u64,
}

impl Debug for WeakExecutorAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "({:?},I{}/{})",
            self.executor_type_id,
            self.executor_internal_id,
            match &self.executor_keeper {
                None => "NA".to_string(),
                Some(ek) => {
                    format!("CN[{}]", ek.strong_count())
                }
            }
        ))
    }
}

unsafe impl Send for WeakExecutorAddress {}
unsafe impl Sync for WeakExecutorAddress {}

impl WeakExecutorAddress {
    pub(crate) fn empty() -> Self {
        Self {
            executor_keeper: None,
            executor_type_id: TypeId::of::<()>(),
            executor_internal_id: 0,
        }
    }

    pub(crate) fn get_strong_count(&self) -> usize {
        self.executor_keeper
            .as_ref()
            .map(|e| e.strong_count())
            .unwrap_or(0)
    }

    pub(crate) fn is_deallocated(&self) -> bool {
        if let Some(e) = &self.executor_keeper {
            e.strong_count() > 0
        } else {
            false
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
        self.executor_type_id.hash(state);
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
