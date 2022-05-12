use crate::execution_manager::executor::{Executor, ExecutorType};
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::executors_list::{ExecutorAllocMode, PoolAllocMode};
use crate::execution_manager::manager::{ExecutionManager, GenericExecutor};
use crate::execution_manager::memory_tracker::MemoryTrackerManager;
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
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
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

enum ExecutorAllocResult {
    AlreadyDeleted,
    PacketMissing,
    NoMorePools,
    Allocated(Vec<GenericExecutor>, ExecutorPriority),
}

pub struct ExecutionManagerInfo {
    executor_type: ExecutorType,
    pub output_pool: Option<Arc<dyn ExecThreadPoolDataAddTrait>>,
    allocator: Option<Box<dyn (Fn(WeakExecutorAddress) -> ExecutorAllocResult) + Sync + Send>>,
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

struct WorkSchedulerInternal {
    waiting_addresses: HashMap<TypeId, VecDeque<WeakExecutorAddress>>,
    priority_list: PriorityManager,
}

pub struct WorkScheduler {
    packets_map:
        Arc<DashMap<WeakExecutorAddress, Arc<(AtomicBool, PoolObject<SegQueue<PacketAny>>)>>>,
    active_executors_counters: HashMap<TypeId, Arc<AtomicU64>>,
    execution_managers_info: HashMap<TypeId, Arc<RwLock<ExecutionManagerInfo>>>,
    memory_tracker: Arc<MemoryTrackerManager>,

    executor_updates_queue: SegQueue<WeakExecutorAddress>,

    internal: Mutex<WorkSchedulerInternal>,

    changes_notifier_condvar: Condvar,

    queues_allocator: ObjectsPool<SegQueue<PacketAny>>,
}

impl<T: Send + Sync + 'static> PoolObjectTrait for SegQueue<T> {
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

            changes_notifier_condvar: Default::default(),
            memory_tracker: Arc::new(MemoryTrackerManager::new()),
            queues_allocator: ObjectsPool::new(queue_buffers_pool_size, ()),
            executor_updates_queue: SegQueue::new(),
            internal: Mutex::new(WorkSchedulerInternal {
                priority_list: PriorityManager::new(),
                waiting_addresses: Default::default(),
            }),
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
                PoolAllocMode::Shared { capacity } => {
                    PoolMode::Shared(Arc::new(PoolObject::new_simple(PacketsPool::new(
                        capacity,
                        pool_init_data,
                        &self.memory_tracker,
                    ))))
                }
                PoolAllocMode::Distinct { capacity } => PoolMode::Distinct {
                    pools_allocator: ObjectsPool::new(
                        executors_max_count * 2,
                        (capacity, pool_init_data, self.memory_tracker.clone()),
                    ),
                },
            },
            executors_allocator: ObjectsPool::new(
                executors_max_count,
                (
                    global_params.clone(),
                    self.memory_tracker.get_executor_instance(),
                ),
            ),
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

        let mem_tracker = self.memory_tracker.clone();

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
                    return ExecutorAllocResult::AlreadyDeleted;
                }
            };

            let pool = match &executors_list_manager_aeu.packet_pools {
                PoolMode::None => None,
                PoolMode::Shared(pool) => Some(pool.clone()),
                PoolMode::Distinct { pools_allocator } => {
                    if pools_allocator.get_available_items() > 0 {
                        Some(Arc::new(pools_allocator.alloc_object(false, false)))
                    } else {
                        return ExecutorAllocResult::NoMorePools;
                    }
                }
            };

            // TODO: Memory params
            let (build_info, maximum_concurrency) = E::allocate_new_group(
                global_params.clone(),
                None,
                if needs_build_packet {
                    Some(
                        match Self::get_packet(addr, &packets_queue, &packets_map).0 {
                            Some(packet) => packet,
                            None => return ExecutorAllocResult::PacketMissing,
                        },
                    )
                } else {
                    None
                },
                |v| {
                    executor_info
                        .output_pool
                        .as_ref()
                        .unwrap()
                        .clone()
                        .add_executors_batch(v);
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
                    .alloc_object(false, false);

                let packets_map = packets_map.clone();
                let packets_queue = packets_queue.clone();

                let queue_size = packets_queue.1.len();

                let mem_tracker = mem_tracker.clone();

                execution_managers.push(ExecutionManager::new(
                    executor,
                    if i == (maximum_concurrency - 1) {
                        build_info.take().unwrap()
                    } else {
                        build_info.as_ref().unwrap().clone()
                    },
                    pool.clone(),
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
                        mem_tracker.add_queue_packet(packet.deref());
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

            ExecutorAllocResult::Allocated(
                execution_managers,
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

        self.internal
            .lock()
            .waiting_addresses
            .insert(TypeId::of::<E>(), VecDeque::new());

        assert!(not_present);
        executor_info
    }

    pub fn register_executors_batch(self: &Arc<Self>, executors: Vec<ExecutorAddress>) {
        let mut internal = self.internal.lock();

        for executor in executors {
            unsafe {
                *(executor.executor_keeper.scheduler.get()) = Arc::downgrade(self);
                *(executor.executor_keeper.weak_addr.get()) = executor.to_weak();
            }

            let old_val = self.packets_map.insert(
                executor.to_weak(),
                Arc::new((
                    AtomicBool::new(false),
                    self.queues_allocator.alloc_object(false, false),
                )),
            );
            self.active_executors_counters
                .get(&executor.executor_type_id)
                .unwrap()
                .fetch_add(1, Ordering::SeqCst);
            assert!(old_val.is_none());

            internal
                .waiting_addresses
                .get_mut(&executor.executor_type_id)
                .unwrap()
                .push_back(executor.to_weak());
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
        self.executor_updates_queue.push(address);
        self.changes_notifier_condvar.notify_all();
    }

    fn alloc_executors(&self, address: WeakExecutorAddress) -> ExecutorAllocResult {
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
        let mut internal = self.internal.lock();
        let internal = internal.deref_mut();

        // Perform the priority updates
        while let Some(update) = self.executor_updates_queue.pop() {
            let length = self
                .packets_map
                .get(&update)
                .map(|x| x.1.len())
                .unwrap_or(0);
            internal
                .priority_list
                .change_priority(&update, |p| p.update_score(length as u64));
        }

        // Allocate as much executors as possible
        for (addr, addr_queue) in internal.waiting_addresses.iter_mut() {
            let mut missed_iterations_count = 0;

            let exec_remaining_func_lock = self.execution_managers_info.get(addr).unwrap().read();

            let exec_remaining_func = exec_remaining_func_lock
                .pool_remaining_executors
                .as_ref()
                .unwrap();

            'alloc_loop: while addr_queue.len() > 0 && exec_remaining_func() > 0 {
                if let Some(addr) = addr_queue.pop_front() {
                    match self.alloc_executors(addr) {
                        ExecutorAllocResult::Allocated(executors, priority) => {
                            if executors.len() > 0 {
                                internal.priority_list.add_elements(executors, priority);
                            }
                        }
                        ExecutorAllocResult::PacketMissing => {
                            addr_queue.push_back(addr);
                            missed_iterations_count += 1;
                            if missed_iterations_count > addr_queue.len() {
                                break 'alloc_loop;
                            }
                        }
                        ExecutorAllocResult::NoMorePools => {
                            addr_queue.push_front(addr);
                            break 'alloc_loop;
                        }
                        ExecutorAllocResult::AlreadyDeleted => {
                            // Do nothing
                        }
                    }
                }
            }
        }

        if let Some(executor) = last_executor {
            if executor.is_finished() {
                internal
                    .priority_list
                    .remove_element(last_executor.take().unwrap().get_weak_address());
            } else {
                internal
                    .priority_list
                    .change_priority(executor.get_weak_address(), |p| {
                        p.update_score(executor.get_packets_queue_size() as u64)
                    });
            }
        }

        *last_executor = internal.priority_list.get_element(last_executor.take());
    }

    pub fn print_debug_memory(&self) {
        self.memory_tracker.print_debug();
    }

    pub fn print_debug_executors(&self) {
        let _out = std::io::stdout().lock();
        let internal = self.internal.lock();
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
                internal.priority_list.has_element_debug(addr),
                exec_remaining
            );
        }
    }

    pub fn wait_for_progress(&self) {
        let mut wait_lock = self.internal.lock();
        self.changes_notifier_condvar
            .wait_for(&mut wait_lock, Duration::from_millis(100));
        drop(wait_lock);
    }

    pub fn finalize(&self) {
        let mut internal = self.internal.lock();
        assert_eq!(self.get_packets_count(), 0);
        internal.priority_list.clear_all();
        self.packets_map.clear();
        for (_, manager) in self.execution_managers_info.iter() {
            manager.write().output_pool.take();
        }
    }
}
