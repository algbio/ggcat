use crate::execution_manager::executor::Executor;
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::executors_list::{ExecutorAllocMode, ExecutorsList, PoolAllocMode};
use crate::execution_manager::manager::{ExecutionManager, ExecutionManagerTrait, GenericExecutor};
use crate::execution_manager::objects_pool::{ObjectsPool, PoolObject, PoolObjectTrait};
use crate::execution_manager::packet::{PacketAny, PacketsPool};
use crate::execution_manager::thread_pool::ExecThreadPoolDataAddTrait;
use crossbeam::queue::{ArrayQueue, SegQueue};
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
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

pub struct WorkManager {
    allocator_functions:
        HashMap<u64, Box<dyn (Fn(&ExecutorAddress) -> Option<GenericExecutor>) + Sync + Send>>,
    packets_map: DashMap<ExecutorAddress, PoolObject<ArrayQueue<PacketAny>>>,

    full_executors: SegQueue<GenericExecutor>,
    available_executors: SegQueue<GenericExecutor>,
    duplicable_executors: SegQueue<WeakExecutorAddress>,

    waiting_addresses: SegQueue<ExecutorAddress>,

    pending_packets_count: AtomicU64,

    output_pool: Arc<RwLock<Option<Arc<dyn ExecThreadPoolDataAddTrait + 'static>>>>,

    changes_notifier_mutex: Mutex<()>,
    changes_notifier_condvar: Condvar,

    queues_allocator: ObjectsPool<ArrayQueue<PacketAny>>,
}

impl<T: 'static> PoolObjectTrait for ArrayQueue<T> {
    type InitData = usize;

    fn allocate_new(capacity: &Self::InitData) -> Self {
        ArrayQueue::new(*capacity)
    }

    fn reset(&mut self) {
        assert!(self.pop().is_none());
    }
}

impl WorkManager {
    pub fn new(queue_buffers_pool_size: usize, executor_buffer_capacity: usize) -> Self {
        Self {
            allocator_functions: HashMap::new(),
            packets_map: Default::default(),
            full_executors: Default::default(),
            available_executors: Default::default(),
            duplicable_executors: Default::default(),
            waiting_addresses: Default::default(),
            pending_packets_count: AtomicU64::new(0),
            output_pool: Arc::new(RwLock::new(None)),
            changes_notifier_mutex: Default::default(),
            changes_notifier_condvar: Default::default(),
            queues_allocator: ObjectsPool::new(
                queue_buffers_pool_size,
                false,
                executor_buffer_capacity,
            ),
        }
    }

    pub fn set_output<P: ExecThreadPoolDataAddTrait + 'static>(&mut self, output_target: Arc<P>) {
        *self.output_pool.write() = Some(output_target);
    }

    pub fn add_executors<E: Executor>(
        &mut self,
        alloc_mode: ExecutorAllocMode,
        pool_alloc_mode: PoolAllocMode,
        pool_init_data: <E::OutputPacket as PoolObjectTrait>::InitData,
        global_params: Arc<E::GlobalParams>,
    ) {
        let executors_max_count = match alloc_mode {
            ExecutorAllocMode::Fixed(count) => count,
            ExecutorAllocMode::MemoryLimited { max_count, .. } => max_count,
        };

        let executors_manager = Arc::new(ExecutorsListManager::<E> {
            packet_pools: match pool_alloc_mode {
                PoolAllocMode::None => PoolMode::None,
                PoolAllocMode::Shared { capacity } => PoolMode::Shared(Arc::new(
                    PoolObject::new_simple(PacketsPool::new(capacity, true, pool_init_data)),
                )),
                PoolAllocMode::Instance { capacity } => PoolMode::Distinct {
                    pools_allocator: ObjectsPool::new(
                        executors_max_count,
                        false,
                        (capacity, true, pool_init_data),
                    ),
                },
            },
            executors_allocator: ObjectsPool::new(executors_max_count, false, ()),
        });

        let output_pool = self.output_pool.clone();

        let allocate_execution_unit = move |addr: &ExecutorAddress| {
            let executor = executors_manager.executors_allocator.alloc_object();

            addr.executor_keeper.try_read().unwrap();

            if let Some(main_executor) = addr.executor_keeper.read().as_ref() {
                return main_executor
                    .downcast_ref::<ExecutionManager<E>>()
                    .unwrap()
                    .clone_executor(executor);
            }

            // TODO: Memory params
            let build_info = E::allocate_new_group(global_params.clone(), None);
            let output_pool = output_pool.clone();
            Some(ExecutionManager::new(
                executor,
                build_info,
                match &executors_manager.packet_pools {
                    PoolMode::None => None,
                    PoolMode::Shared(pool) => Some(pool.clone()),
                    PoolMode::Distinct { pools_allocator } => {
                        Some(Arc::new(pools_allocator.alloc_object()))
                    }
                },
                addr.clone(),
                move |addr, packet| {
                    output_pool
                        .read()
                        .as_ref()
                        .unwrap()
                        .add_data(addr, packet.upcast());
                },
            ))
        };

        let not_present = self
            .allocator_functions
            .insert(E::EXECUTOR_TYPE_INDEX, Box::new(allocate_execution_unit))
            .is_none();
        assert!(not_present);
    }

    pub fn add_input_packet(&self, address: ExecutorAddress, mut packet: PacketAny) {
        self.pending_packets_count.fetch_add(1, Ordering::SeqCst);
        loop {
            match self
                .packets_map
                .entry(address.clone())
                .or_insert_with(|| {
                    self.waiting_addresses.push(address.clone());
                    self.queues_allocator.alloc_object()
                })
                .get_value()
                .push(packet)
            {
                Ok(_) => {
                    self.changes_notifier_condvar.notify_all();
                    break;
                }
                Err(val) => packet = val,
            }
            println!("Failed packet insertion!");
        }
    }

    fn alloc_executor(&self, address: &ExecutorAddress) -> Option<GenericExecutor> {
        (self
            .allocator_functions
            .get(&address.executor_type_id)
            .unwrap())(address)
    }

    fn get_packet_from_addr(&self, addr: &ExecutorAddress) -> Option<PacketAny> {
        match self.packets_map.get(&addr) {
            None => None,
            Some(packets) => {
                if let Some(packet) = packets.value().get_value().pop() {
                    Some(packet)
                } else {
                    None
                }
            }
        }
    }

    pub fn find_work(&self, last_executor: &mut Option<GenericExecutor>) -> Option<PacketAny> {
        // if self.pending_packets_count.load(Ordering::SeqCst) == 0 {
        //     let mut wait_lock = self.changes_notifier_mutex.lock();
        //     self.changes_notifier_condvar
        //         .wait_for(&mut wait_lock, Duration::from_millis(100));
        // }

        while self.pending_packets_count.load(Ordering::SeqCst) > 0 {
            if let Some(executor) = last_executor {
                let strong_addr = executor.get_address();
                if let Some(packet) = self.get_packet_from_addr(&strong_addr) {
                    self.pending_packets_count.fetch_sub(1, Ordering::Relaxed);
                    self.changes_notifier_condvar.notify_all();
                    return Some(packet);
                }
            }

            while let Some(weak_addr) = self.duplicable_executors.pop() {
                // println!(
                //     "Duplicate work... {} / {:?}",
                //     std::any::type_name::<I>(),
                //     std::thread::current().id()
                // );
                if let Some(addr) = weak_addr.get_strong() {
                    if let Some(executor) = &last_executor {
                        if addr == executor.get_address() {
                            self.duplicable_executors.push(weak_addr);
                            if self.duplicable_executors.len() == 1 {
                                break;
                            } else {
                                continue;
                            }
                        }
                    }

                    if let Some(executor) = self.alloc_executor(&addr) {
                        if let Some(packet) = self.get_packet_from_addr(&addr) {
                            self.duplicable_executors.push(weak_addr);

                            self.pending_packets_count.fetch_sub(1, Ordering::Relaxed);
                            self.changes_notifier_condvar.notify_all();
                            // println!(
                            //     "Found work... {} / {:?}",
                            //     std::any::type_name::<I>(),
                            //     std::thread::current().id()
                            // );
                            *last_executor = Some(executor);
                            return Some(packet);
                        }
                    }
                }
            }

            if let Some(addr) = self.waiting_addresses.pop() {
                // println!(
                //     "Waiting work... {} / {:?}",
                //     std::any::type_name::<I>(),
                //     std::thread::current().id()
                // );
                let executor = self.alloc_executor(&addr).unwrap();
                // println!(
                //     "Allocated executor! {} / {:?}",
                //     std::any::type_name::<I>(),
                //     std::thread::current().id()
                // );

                if executor.can_split() {
                    self.duplicable_executors.push(addr.to_weak());
                    self.changes_notifier_condvar.notify_all();
                }

                if let Some(packet) = self.get_packet_from_addr(&addr) {
                    // println!(
                    //     "Found work... {} / {:?}",
                    //     std::any::type_name::<I>(),
                    //     std::thread::current().id()
                    // );
                    self.pending_packets_count.fetch_sub(1, Ordering::Relaxed);
                    self.changes_notifier_condvar.notify_all();
                    *last_executor = Some(executor);
                    return Some(packet);
                }
            }

            let mut wait_lock = self.changes_notifier_mutex.lock();

            self.changes_notifier_condvar
                .wait_for(&mut wait_lock, Duration::from_millis(100));
        }

        // Strategy idea:
        // 1) if an executor is too full and current is not, switch to it --> List of 'full' executors
        // 2) if last executed executor has work, run it --> Index packets by address
        // 3) find an allocated executor that has some work to do --> Atomic queue of executors with work to do + flag to indicate presence in queue
        // 4) try to duplicate executors on a group --> Map of executors that can be duplicated
        // 5) execute another group --> Group allocation + list of executor addresses to start
        // 6) Low / high priority

        // Needed functions:
        // 1) AllocateExecutor(address)
        // 2)
        // println!("Returned None!");
        None
    }
}
