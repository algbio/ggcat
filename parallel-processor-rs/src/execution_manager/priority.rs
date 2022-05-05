use crate::execution_manager::executor_address::WeakExecutorAddress;
use crate::execution_manager::manager::ExecutionManagerTrait;
use parking_lot::Mutex;
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

const CURRENT_ADDITIONAL_SCORE_VALUE: u64 = 2;

#[derive(Clone)]
pub struct ExecutorPriority {
    packets_multiplier: u64,
    base_offset: u64,
    pub total_score: u64,
    unique_id: i64,
}

static UNIQUE_ID: AtomicI64 = AtomicI64::new(0);

impl ExecutorPriority {
    pub fn empty() -> Self {
        Self {
            packets_multiplier: 0,
            base_offset: 0,
            total_score: 0,
            unique_id: 0,
        }
    }

    pub fn new(packets_multiplier: u64, packets_count: u64, base_offset: u64) -> Self {
        let mut self_ = Self {
            packets_multiplier,
            base_offset,
            total_score: 0,
            unique_id: 0,
        };

        self_.update_score(packets_count);

        self_
    }

    pub fn update_score(&mut self, packets_count: u64) {
        self.unique_id = UNIQUE_ID.fetch_sub(1, Ordering::Relaxed);
        self.total_score = self.packets_multiplier * packets_count + self.base_offset;
    }
}

impl PartialOrd for ExecutorPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let e1 = (self.total_score, self.unique_id);
        let e2 = (other.total_score, other.unique_id);
        e1.partial_cmp(&e2)
    }
}

impl Ord for ExecutorPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let e1 = (self.total_score, self.unique_id);
        let e2 = (other.total_score, other.unique_id);
        e1.cmp(&e2)
    }
}

impl PartialEq for ExecutorPriority {
    fn eq(&self, other: &Self) -> bool {
        let e1 = (self.total_score, self.unique_id);
        let e2 = (other.total_score, other.unique_id);
        e1 == e2
    }
}

impl Eq for ExecutorPriority {}

struct PriorityManagerInternal {
    priority_map: BTreeMap<ExecutorPriority, Vec<Box<dyn ExecutionManagerTrait>>>,
    reverse_map: HashMap<WeakExecutorAddress, ExecutorPriority>,
}

pub struct PriorityManager {
    internal: Mutex<PriorityManagerInternal>,
}

impl PriorityManager {
    pub fn new() -> Self {
        Self {
            internal: Mutex::new(PriorityManagerInternal {
                priority_map: BTreeMap::new(),
                reverse_map: HashMap::new(),
            }),
        }
    }

    pub fn add_elements(
        &self,
        elements: Vec<Box<dyn ExecutionManagerTrait>>,
        priority: ExecutorPriority,
    ) {
        let key: &WeakExecutorAddress = elements[0].as_ref().borrow();
        let key = key.clone();

        let mut internal = self.internal.lock();

        let old_el = internal.priority_map.insert(priority.clone(), elements);
        assert!(old_el.is_none());

        internal.reverse_map.insert(key, priority.clone());
    }

    pub fn change_priority(
        &self,
        key: &WeakExecutorAddress,
        change_fn: impl FnOnce(&mut ExecutorPriority),
    ) {
        let mut internal = self.internal.lock();
        let mut internal = internal.deref_mut();

        if let Some(priority) = internal.reverse_map.get_mut(key) {
            if let Some((_, value)) = internal.priority_map.remove_entry(priority) {
                change_fn(priority);
                internal.priority_map.insert(priority.clone(), value);
            } else {
                change_fn(priority);
            }
        }
    }

    pub fn elements_count_debug(&self) -> usize {
        let internal = self.internal.lock();
        std::cmp::max(internal.reverse_map.len(), internal.priority_map.len())
    }

    pub fn has_element_debug(&self, el: &WeakExecutorAddress) -> (bool, bool, u64) {
        let il = self.internal.lock();
        if let Some(prio) = il.reverse_map.get(el) {
            (true, il.priority_map.contains_key(prio), prio.total_score)
        } else {
            (false, false, 0)
        }
    }

    pub fn get_element(
        &self,
        return_element: Option<Box<dyn ExecutionManagerTrait>>,
    ) -> Option<Box<dyn ExecutionManagerTrait>> {
        let mut internal = self.internal.lock();
        let mut internal = internal.deref_mut();

        if let Some(mut entry) = internal.priority_map.last_entry() {
            let new_score = entry.key().total_score;

            let new_entry;

            if let Some(ret_element) = return_element {
                let priority = internal
                    .reverse_map
                    .get(ret_element.deref().borrow())?
                    .clone();

                let mut score = priority.total_score;

                if score > 0 {
                    score += priority.packets_multiplier * CURRENT_ADDITIONAL_SCORE_VALUE;
                }

                if score > new_score {
                    return Some(ret_element);
                } else {
                    new_entry = entry.get_mut().pop().unwrap();
                    if entry.get().len() == 0 {
                        entry.remove();
                    }
                    internal
                        .priority_map
                        .entry(priority)
                        .or_insert_with(|| Vec::new())
                        .push(ret_element);
                }
            } else {
                new_entry = entry.get_mut().pop().unwrap();
                if entry.get().len() == 0 {
                    entry.remove();
                }
            }

            Some(new_entry)
        } else {
            return_element
        }
    }

    pub fn remove_element(&self, addr: &WeakExecutorAddress) {
        let mut internal = self.internal.lock();

        if let Some(prio) = internal.reverse_map.remove(addr) {
            internal.priority_map.remove(&prio);
        }
    }

    pub fn clear_all(&self) {
        let mut internal = self.internal.lock();
        internal.priority_map.clear();
        internal.reverse_map.clear();
    }
}
