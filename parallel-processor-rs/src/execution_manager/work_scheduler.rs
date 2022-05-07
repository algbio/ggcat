use crate::execution_manager::executor::{Executor, ExecutorType};
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::executors_list::{ExecutorAllocMode, PoolAllocMode};
use crate::execution_manager::manager::{ExecutionManager, GenericExecutor};
use crate::execution_manager::objects_pool::{ObjectsPool, PoolObject, PoolObjectTrait};
use crate::execution_manager::packet::{Packet, PacketAny, PacketsPool};
use crate::execution_manager::priority::{ExecutorPriority, PriorityManager};
use crate::execution_manager::thread_pool::ExecThreadPoolDataAddTrait;
use crossbeam::queue::SegQueue;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use parking_lot::{Condvar, Mutex, RwLock};
use std::any::TypeId;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

enum PoolMode<E: Executor> {
    None,
    Shared(Arc<PoolObject<PacketsPool<E::OutputPacket>>>),
    Distinct {
        pools_allocator: ObjectsPool<PacketsPool<E::OutputPacket>>,
    },
}

struct ExecutorsListManager<E: Executor> {
    packet_pools: PoolMode<E>,
    executors_allocator: ObjectsPool<E>,
}

pub struct ExecutionManagerInfo {
    executor_type: ExecutorType,
    pub output_pool: Option<Arc<dyn ExecThreadPoolDataAddTrait>>,
    allocator: Option<
        Box<
            dyn (Fn(
                    WeakExecutorAddress,
                ) -> (
                    Vec<GenericExecutor>,
                    Option<(Arc<dyn ExecThreadPoolDataAddTrait>, Vec<ExecutorAddress>)>,
                    ExecutorPriority,
                )) + Sync
                + Send,
        >,
    >,
    pool_remaining_executors: Option<Box<dyn (Fn() -> i64) + Sync + Send>>,
}

pub struct ExecutorDropper {
    weak_addr: UnsafeCell<WeakExecutorAddress>,
    scheduler: UnsafeCell<Weak<WorkScheduler>>,
}

impl ExecutorDropper {
    pub fn new() -> Self {
        Self {
            weak_addr: UnsafeCell::new(WeakExecutorAddress::empty()),
            scheduler: UnsafeCell::new(Weak::new()),
        }
    }
}

impl Drop for ExecutorDropper {
    fn drop(&mut self) {
        let scheduler = unsafe { &*(self.scheduler.get()) };
        if let Some(scheduler) = scheduler.upgrade() {
            let address = unsafe { &*(self.weak_addr.get()) };
            scheduler.dealloc_address(address.clone());
        }
    }
}

pub struct WorkScheduler {
    execution_managers_info: HashMap<TypeId, Arc<RwLock<ExecutionManagerInfo>>>,
    packets_map:
        Arc<DashMap<WeakExecutorAddress, Arc<(AtomicBool, PoolObject<SegQueue<PacketAny>>)>>>,
    active_executors_counters: HashMap<TypeId, Arc<AtomicU64>>,

    priority_list: PriorityManager,
    waiting_addresses: HashMap<TypeId, RwLock<SegQueue<WeakExecutorAddress>>>,

    changes_notifier_mutex: Mutex<()>,
    changes_notifier_condvar: Condvar,

    queues_allocator: ObjectsPool<SegQueue<PacketAny>>,
    push_executors_lock: Mutex<()>,
}

impl<T: 'static> PoolObjectTrait for SegQueue<T> {
    type InitData = ();

    fn allocate_new(_: &Self::InitData) -> Self {
        SegQueue::new()
    }

    fn reset(&mut self) {
        assert!(self.pop().is_none());
    }
}

impl WorkScheduler {
    pub fn new(queue_buffers_pool_size: usize) -> Self {
        Self {
            execution_managers_info: HashMap::new(),
            packets_map: Default::default(),
            active_executors_counters: Default::default(),
            priority_list: PriorityManager::new(),
            waiting_addresses: Default::default(),
            changes_notifier_mutex: Default::default(),
            changes_notifier_condvar: Default::default(),
            queues_allocator: ObjectsPool::new(queue_buffers_pool_size, ()),
            push_executors_lock: Mutex::new(()),
        }
    }

    fn dealloc_address(&self, addr: WeakExecutorAddress) {
        let entry = self.packets_map.entry(addr);
        if let Entry::Occupied(o) = entry {
            let entry = o.get();

            entry.0.store(true, Ordering::SeqCst);

            if entry.1.len() == 0 {
                o.remove_entry();
            }
        }
    }

    pub fn add_executors<E: Executor>(
        &mut self,
        alloc_mode: ExecutorAllocMode,
        pool_alloc_mode: PoolAllocMode,
        pool_init_data: <E::OutputPacket as PoolObjectTrait>::InitData,
        global_params: Arc<E::GlobalParams>,
    ) -> Arc<RwLock<ExecutionManagerInfo>> {
        let executors_max_count = match alloc_mode {
            ExecutorAllocMode::Fixed(count) => count,
            ExecutorAllocMode::MemoryLimited { max_count, .. } => max_count,
        };

        let executors_list_manager = Arc::new(ExecutorsListManager::<E> {
            packet_pools: match pool_alloc_mode {
                PoolAllocMode::None => PoolMode::None,
                PoolAllocMode::Shared { capacity } => PoolMode::Shared(Arc::new(
                    PoolObject::new_simple(PacketsPool::new(capacity, pool_init_data)),
                )),
                PoolAllocMode::Distinct { capacity } => PoolMode::Distinct {
                    pools_allocator: ObjectsPool::new(
                        executors_max_count,
                        (capacity, pool_init_data),
                    ),
                },
            },
            executors_allocator: ObjectsPool::new(executors_max_count, ()),
        });

        let executor_info = Arc::new(RwLock::new(ExecutionManagerInfo {
            executor_type: E::EXECUTOR_TYPE,
            output_pool: None,
            allocator: None,
            pool_remaining_executors: None,
        }));

        let executor_info_aeu = Arc::downgrade(&executor_info);
        let packets_map = self.packets_map.clone();

        let executors_list_manager_aeu = executors_list_manager.clone();

        self.active_executors_counters
            .insert(TypeId::of::<E>(), Arc::new(AtomicU64::new(0)));

        let active_counter = self
            .active_executors_counters
            .get(&TypeId::of::<E>())
            .unwrap()
            .clone();

        let allocate_execution_units = move |addr: WeakExecutorAddress| {
            let executor_info = executor_info_aeu.upgrade().unwrap();
            let executor_info = executor_info.read();
            let executor_info = executor_info.deref();

            let needs_build_packet = executor_info.executor_type == ExecutorType::NeedsInitPacket;

            let packets_queue = match packets_map.get(&addr) {
                Some(map) => map.clone(),
                None => {
                    // Do not do anything if we are deallocated without any packet
                    active_counter.fetch_sub(1, Ordering::Relaxed);
                    return (Vec::new(), None, ExecutorPriority::empty());
                }
            };

            let mut new_addresses = None;

            // TODO: Memory params
            let (build_info, maximum_concurrency) = E::allocate_new_group(
                global_params.clone(),
                None,
                if needs_build_packet {
                    Some(
                        Self::get_packet(addr, &packets_queue, &packets_map)
                            .0
                            .unwrap(),
                    )
                } else {
                    None
                },
                |v| {
                    new_addresses = Some((executor_info.output_pool.as_ref().unwrap().clone(), v));
                },
            );

            let mut build_info = Some(build_info);

            assert!(maximum_concurrency > 0);
            let mut execution_managers = Vec::with_capacity(maximum_concurrency);
            active_counter.fetch_add(maximum_concurrency as u64 - 1, Ordering::SeqCst);

            for i in 0..maximum_concurrency {
                let active_counter = active_counter.clone();
                let output_pool = executor_info.output_pool.clone();
                let executor = executors_list_manager_aeu
                    .executors_allocator
                    .alloc_object(false);

                let packets_map = packets_map.clone();
                let packets_queue = packets_queue.clone();

                let queue_size = packets_queue.1.len();

                execution_managers.push(ExecutionManager::new(
                    executor,
                    if i == (maximum_concurrency - 1) {
                        build_info.take().unwrap()
                    } else {
                        build_info.as_ref().unwrap().clone()
                    },
                    match &executors_list_manager_aeu.packet_pools {
                        PoolMode::None => None,
                        PoolMode::Shared(pool) => Some(pool.clone()),
                        PoolMode::Distinct { pools_allocator } => {
                            Some(Arc::new(pools_allocator.alloc_object(false)))
                        }
                    },
                    Box::new(move || {
                        let (packet, del) = Self::get_packet(addr, &packets_queue, &packets_map);
                        (
                            packet,
                            if del {
                                None
                            } else {
                                Some(packets_queue.1.len())
                            },
                        )
                    }),
                    queue_size,
                    addr.clone(),
                    move |addr, packet| {
                        output_pool
                            .as_ref()
                            .unwrap()
                            .add_data(addr.to_weak(), packet.upcast());
                    },
                    move || {
                        active_counter.fetch_sub(1, Ordering::SeqCst);
                    },
                ));
            }

            (
                execution_managers,
                new_addresses,
                ExecutorPriority::new(
                    E::PACKET_PRIORITY_MULTIPLIER,
                    packets_queue.1.len() as u64,
                    E::BASE_PRIORITY,
                ),
            )
        };

        let pool_remaining_executors = move || {
            executors_list_manager
                .executors_allocator
                .get_available_items()
        };

        let mut ei = executor_info.write();
        ei.allocator = Some(Box::new(allocate_execution_units));
        ei.pool_remaining_executors = Some(Box::new(pool_remaining_executors));
        drop(ei);

        let not_present = self
            .execution_managers_info
            .insert(TypeId::of::<E>(), executor_info.clone())
            .is_none();

        self.waiting_addresses
            .insert(TypeId::of::<E>(), RwLock::new(SegQueue::new()));

        assert!(not_present);
        executor_info
    }

    pub fn register_executors_batch(self: &Arc<Self>, mut executors: Vec<ExecutorAddress>) {
        let _push_executors_lock = self.push_executors_lock.lock();

        while executors.len() > 0 {
            let first_type = executors[0].executor_type_id;

            let exec_queue = self.waiting_addresses.get(&first_type).unwrap().write();

            executors
                .drain_filter(|e| e.executor_type_id == first_type)
                .for_each(|executor| {
                    unsafe {
                        *(executor.executor_keeper.scheduler.get()) = Arc::downgrade(self);
                        *(executor.executor_keeper.weak_addr.get()) = executor.to_weak();
                    }

                    let old_val = self.packets_map.insert(
                        executor.to_weak(),
                        Arc::new((
                            AtomicBool::new(false),
                            self.queues_allocator.alloc_object(false),
                        )),
                    );
                    self.active_executors_counters
                        .get(&executor.executor_type_id)
                        .unwrap()
                        .fetch_add(1, Ordering::SeqCst);
                    assert!(old_val.is_none());
                    exec_queue.push(executor.to_weak());
                });
        }
    }

    pub fn get_packets_count(&self) -> usize {
        self.packets_map
            .iter()
            .map(|p| p.value().1.len())
            .sum::<usize>()
    }

    pub fn add_input_packet(&self, address: WeakExecutorAddress, packet: PacketAny) {
        let queue = &self.packets_map.get(&address).unwrap().1;
        queue.push(packet);
        self.priority_list
            .change_priority(&address, |p| p.update_score(queue.len() as u64));
        self.changes_notifier_condvar.notify_all();
    }

    fn alloc_executors(
        &self,
        address: WeakExecutorAddress,
    ) -> (
        Vec<GenericExecutor>,
        Option<(Arc<dyn ExecThreadPoolDataAddTrait>, Vec<ExecutorAddress>)>,
        ExecutorPriority,
    ) {
        let executor_info = self
            .execution_managers_info
            .get(&address.executor_type_id)
            .unwrap()
            .read();

        (executor_info.allocator.as_ref().unwrap())(address.clone())
    }

    fn get_packet<T>(
        addr: WeakExecutorAddress,
        queue: &Arc<(AtomicBool, PoolObject<SegQueue<PacketAny>>)>,
        packets_map: &Arc<
            DashMap<WeakExecutorAddress, Arc<(AtomicBool, PoolObject<SegQueue<PacketAny>>)>>,
        >,
    ) -> (Option<Packet<T>>, bool) {
        let packet = queue.1.pop();
        let is_finished = if queue.1.len() == 0 && queue.0.load(Ordering::Relaxed) {
            packets_map.remove(&addr);
            true
        } else {
            false
        };

        (packet.map(|p| p.downcast()), is_finished)
    }

    pub fn get_allocated_executors(&self, executor_type_id: &TypeId) -> u64 {
        self.active_executors_counters
            .get(executor_type_id)
            .unwrap()
            .load(Ordering::SeqCst)
    }

    pub fn maybe_change_work(&self, last_executor: &mut Option<GenericExecutor>) {
        loop {
            // Allocate as much executors as possible
            for (addr, addr_queue) in self.waiting_addresses.iter() {
                let addr_queue = addr_queue.read();
                while addr_queue.len() > 0 {
                    let exec_remaining = (self
                        .execution_managers_info
                        .get(addr)
                        .unwrap()
                        .read()
                        .pool_remaining_executors
                        .as_ref()
                        .unwrap())();
                    if exec_remaining > 0 {
                        if let Some(addr) = addr_queue.pop() {
                            let (executors, new_addresses, priority) = self.alloc_executors(addr);
                            if executors.len() > 0 {
                                self.priority_list.add_elements_with_atomic_func(
                                    || {
                                        if let Some((output_pool, addresses)) = new_addresses {
                                            output_pool.add_executors_batch(addresses);
                                        }
                                        executors
                                    },
                                    priority,
                                );
                            }
                        }
                    } else {
                        break;
                    }
                }
            }

            if let Some(executor) = last_executor {
                if executor.is_finished() {
                    self.priority_list
                        .remove_element(last_executor.take().unwrap().get_weak_address());
                } else {
                    self.priority_list
                        .change_priority(executor.get_weak_address(), |p| {
                            p.update_score(executor.get_packets_queue_size() as u64)
                        });
                }
            }

            if let Some(new_executor) = self.priority_list.get_element(last_executor.take()) {
                // println!(
                //     "Running executing with priority: {}/{} // {:?} {:?}",
                //     new_executor.0.total_score.load(Ordering::Relaxed),
                //     new_executor.1.is_finished(),
                //     new_executor.1.get_weak_address(),
                //     std::thread::current().id()
                // );
                //

                *last_executor = Some(new_executor);
                // );
                break;
            }

            self.wait_for_progress();

            if self.packets_map.len() == 0 {
                break;
            }
        }
    }

    pub fn print_debug_executors(&self) {
        let _out = std::io::stdout().lock();
        println!("Executors counters:");
        for (addr, exec_remaining) in self.execution_managers_info.iter() {
            println!(
                "Remaining for executor: {:?} ==> {}",
                addr,
                (exec_remaining
                    .read()
                    .pool_remaining_executors
                    .as_ref()
                    .unwrap())()
            );
        }

        println!("Executors status:");
        for pmap in self.packets_map.iter() {
            let addr = pmap.key();
            let value = pmap.deref();
            let value = value.deref();
            let value = value.deref();

            let exec_remaining = (self
                .execution_managers_info
                .get(&addr.executor_type_id)
                .unwrap()
                .read()
                .pool_remaining_executors
                .as_ref()
                .unwrap())();

            println!(
                "Address: {:?} => {:?} // {:?} ER: {}",
                addr,
                value.1.len(),
                self.priority_list.has_element_debug(addr),
                exec_remaining
            );
        }
    }

    pub fn wait_for_progress(&self) {
        let mut wait_lock = self.changes_notifier_mutex.lock();
        self.changes_notifier_condvar
            .wait_for(&mut wait_lock, Duration::from_millis(100));
        drop(wait_lock);
    }

    pub fn finalize(&self) {
        assert_eq!(self.get_packets_count(), 0);
        self.priority_list.clear_all();
        self.packets_map.clear();
        for (_, manager) in self.execution_managers_info.iter() {
            manager.write().output_pool.take();
        }
    }
}
