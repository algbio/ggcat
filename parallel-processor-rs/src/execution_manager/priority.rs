use crate::execution_manager::executor_address::WeakExecutorAddress;
use crate::execution_manager::manager::ExecutionManagerTrait;
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::atomic::{AtomicI64, Ordering};

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

    pub fn update_id(&mut self) {
        self.unique_id = UNIQUE_ID.fetch_sub(1, Ordering::Relaxed);
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

pub struct PriorityManager {
    priority_map: BTreeMap<ExecutorPriority, Vec<Box<dyn ExecutionManagerTrait>>>,
    reverse_map: HashMap<WeakExecutorAddress, ExecutorPriority>,
}

impl PriorityManager {
    pub fn new() -> Self {
        Self {
            priority_map: BTreeMap::new(),
            reverse_map: HashMap::new(),
        }
    }

    pub fn add_elements(
        &mut self,
        elements: Vec<Box<dyn ExecutionManagerTrait>>,
        mut priority: ExecutorPriority,
    ) {
        priority.update_id();

        let key: &WeakExecutorAddress = elements[0].as_ref().borrow();
        let key = key.clone();

        let old_el = self.priority_map.insert(priority.clone(), elements);
        assert!(old_el.is_none());

        self.reverse_map.insert(key, priority.clone());
    }

    pub fn change_priority(
        &mut self,
        key: &WeakExecutorAddress,
        change_fn: impl FnOnce(&mut ExecutorPriority),
    ) {
        if let Some(priority) = self.reverse_map.get_mut(key) {
            if let Some((_, value)) = self.priority_map.remove_entry(priority) {
                change_fn(priority);
                self.priority_map.insert(priority.clone(), value);
            } else {
                change_fn(priority);
            }
        }
    }

    pub fn elements_count_debug(&self) -> usize {
        std::cmp::max(self.reverse_map.len(), self.priority_map.len())
    }

    pub fn has_element_debug(&self, el: &WeakExecutorAddress) -> (bool, bool, u64) {
        if let Some(prio) = self.reverse_map.get(el) {
            (true, self.priority_map.contains_key(prio), prio.total_score)
        } else {
            (false, false, 0)
        }
    }

    pub fn get_element(
        &mut self,
        return_element: Option<Box<dyn ExecutionManagerTrait>>,
    ) -> Option<Box<dyn ExecutionManagerTrait>> {
        if let Some(mut entry) = self.priority_map.last_entry() {
            let new_score = entry.key().total_score;

            let new_entry;

            if let Some(ret_element) = return_element {
                let priority = self.reverse_map.get(ret_element.deref().borrow())?.clone();

                let mut score = priority.total_score;

                if score > priority.base_offset {
                    score += priority.packets_multiplier * CURRENT_ADDITIONAL_SCORE_VALUE;
                }

                if score > new_score {
                    return Some(ret_element);
                } else {
                    new_entry = entry.get_mut().pop().unwrap();
                    if entry.get().len() == 0 {
                        entry.remove();
                    }
                    self.priority_map
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

    pub fn remove_element(&mut self, addr: &WeakExecutorAddress) {
        if let Some(prio) = self.reverse_map.remove(addr) {
            self.priority_map.remove(&prio);
        }
    }

    pub fn finalize(&mut self) {
        assert_eq!(self.priority_map.len(), 0);
        assert_eq!(self.reverse_map.len(), 0);
    }
}
