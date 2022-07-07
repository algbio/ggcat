use crate::execution_manager::execution_context::ExecutorDropper;
use std::any::TypeId;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone)]
pub struct ExecutorAddress {
    pub(crate) executor_keeper: Arc<ExecutorDropper>,
    pub(crate) executor_type_id: TypeId,
    pub(crate) executor_internal_id: u64,
}
unsafe impl Send for ExecutorAddress {}
unsafe impl Sync for ExecutorAddress {}

impl ExecutorAddress {
    pub fn is_of_type<T: 'static>(&self) -> bool {
        self.executor_type_id == TypeId::of::<T>()
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

#[derive(Copy, Clone)]
pub struct WeakExecutorAddress {
    pub(crate) executor_type_id: TypeId,
    pub(crate) executor_internal_id: u64,
}

impl Debug for WeakExecutorAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "({:?},I{})",
            self.executor_type_id, self.executor_internal_id,
        ))
    }
}

impl WeakExecutorAddress {
    pub(crate) fn empty() -> Self {
        Self {
            executor_type_id: TypeId::of::<()>(),
            executor_internal_id: 0,
        }
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
