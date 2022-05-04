use crate::execution_manager::executor_address::WeakExecutorAddress;
use parking_lot::Mutex;
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

const CURRENT_ADDITIONAL_SCORE_VALUE: u64 = 2;

pub struct ExecutorPriority {
    pub total_score: AtomicU64,
    unique_id: u64,
}

static UNIQUE_ID: AtomicU64 = AtomicU64::new(0);

impl ExecutorPriority {
    pub fn new(score: u64) -> Self {
        Self {
            total_score: AtomicU64::new(score),
            unique_id: UNIQUE_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn update_score(&self, new_score: u64) {
        self.total_score.store(new_score, Ordering::Relaxed);
    }
}

impl PartialOrd for ExecutorPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let e1 = (self.total_score.load(Ordering::Relaxed), self.unique_id);
        let e2 = (other.total_score.load(Ordering::Relaxed), other.unique_id);
        e1.partial_cmp(&e2)
    }
}

impl Ord for ExecutorPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let e1 = (self.total_score.load(Ordering::Relaxed), self.unique_id);
        let e2 = (other.total_score.load(Ordering::Relaxed), other.unique_id);
        e1.cmp(&e2)
    }
}

impl PartialEq for ExecutorPriority {
    fn eq(&self, other: &Self) -> bool {
        let e1 = (self.total_score.load(Ordering::Relaxed), self.unique_id);
        let e2 = (other.total_score.load(Ordering::Relaxed), other.unique_id);
        e1 == e2
    }
}

impl Eq for ExecutorPriority {}

struct PriorityManagerInternal<A: Hash + Eq + Clone, T: Hash + Eq + ?Sized + Borrow<A>> {
    priority_map: BTreeMap<Arc<ExecutorPriority>, Vec<Box<T>>>,
    reverse_map: HashMap<A, Arc<ExecutorPriority>>,
}

pub struct PriorityManager<A: Hash + Eq + Clone, T: Hash + Eq + ?Sized + Borrow<A>> {
    internal: Mutex<PriorityManagerInternal<A, T>>,
}

impl<A: Hash + Eq + Clone, T: Hash + Eq + ?Sized + Borrow<A>> PriorityManager<A, T> {
    pub fn new() -> Self {
        Self {
            internal: Mutex::new(PriorityManagerInternal {
                priority_map: BTreeMap::new(),
                reverse_map: HashMap::new(),
            }),
        }
    }

    pub fn add_elements(&self, elements: Vec<Box<T>>, priority: ExecutorPriority) {
        let priority = Arc::new(priority);
        let key: &A = elements[0].as_ref().borrow();
        let key = key.clone();

        let mut internal = self.internal.lock();

        let old_el = internal.priority_map.insert(priority.clone(), elements);
        assert!(old_el.is_none());

        internal.reverse_map.insert(key, priority.clone());
    }

    pub fn change_priority(&self, key: &A, change_fn: impl FnOnce(&ExecutorPriority)) {
        let mut internal = self.internal.lock();
        let mut internal = internal.deref_mut();

        if let Some(priority) = internal.reverse_map.get(key) {
            if let Some((priority, value)) = internal.priority_map.remove_entry(priority) {
                change_fn(&priority);
                internal.priority_map.insert(priority, value);
            } else {
                change_fn(priority);
            }
        }
    }

    pub fn has_element(&self, el: &A) -> (bool, bool, u64) {
        let il = self.internal.lock();
        if let Some(prio) = il.reverse_map.get(el) {
            (
                true,
                il.priority_map.contains_key(prio),
                prio.total_score.load(Ordering::Relaxed),
            )
        } else {
            (false, false, 0)
        }
    }

    pub fn get_element(
        &self,
        return_element: Option<Box<T>>,
    ) -> Option<(Arc<ExecutorPriority>, Box<T>)> {
        let mut internal = self.internal.lock();
        let mut internal = internal.deref_mut();

        if let Some(mut entry) = internal.priority_map.last_entry() {
            let new_score = entry.key().total_score.load(Ordering::Relaxed);

            let new_entry;

            if let Some(ret_element) = return_element {
                let priority = internal
                    .reverse_map
                    .get(ret_element.deref().borrow())?
                    .clone();

                let mut score = priority.total_score.load(Ordering::Relaxed);

                if score > 0 {
                    score += CURRENT_ADDITIONAL_SCORE_VALUE;
                }

                if score > new_score {
                    return Some((priority, ret_element));
                } else {
                    new_entry = (entry.key().clone(), entry.get_mut().pop().unwrap());
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
                new_entry = (entry.key().clone(), entry.get_mut().pop().unwrap());
                if entry.get().len() == 0 {
                    entry.remove();
                }
            }

            Some(new_entry)
        } else {
            return_element
                .map(|r| {
                    let priority = internal.reverse_map.get(r.deref().borrow())?;
                    Some((priority.clone(), r))
                })
                .flatten()
        }
    }

    pub fn remove_element(&self, el: Box<T>) {
        let mut internal = self.internal.lock();

        if let Some(prio) = internal.reverse_map.get(el.as_ref().borrow()) {
            if !internal.priority_map.contains_key(prio) {
                internal.reverse_map.remove(el.as_ref().borrow());
            }
        }
    }
}
