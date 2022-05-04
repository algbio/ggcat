use crate::execution_manager::executor::{Executor, ExecutorType};
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
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
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

struct ExecutionManagerInfo {
    executor_type: ExecutorType,
    output_type_id: TypeId,
    output_pool: Option<Arc<dyn ExecThreadPoolDataAddTrait>>,
    allocator: Option<
        Box<
            dyn (Fn(&ExecutorAddress, &mut Option<PacketAny>) -> Option<GenericExecutor>)
                + Sync
                + Send,
        >,
    >,
}

pub struct WorkManager {
    execution_managers_info: HashMap<TypeId, Arc<RwLock<ExecutionManagerInfo>>>,
    packets_map: DashMap<WeakExecutorAddress, PoolObject<ArrayQueue<(ExecutorAddress, PacketAny)>>>,

    full_executors: SegQueue<GenericExecutor>,
    available_executors: SegQueue<GenericExecutor>,
    duplicable_executors: SegQueue<WeakExecutorAddress>,

    holding_allocated: HashMap<TypeId, SegQueue<Weak<dyn ExecutionManagerTrait>>>,
    waiting_addresses: HashMap<TypeId, RwLock<SegQueue<ExecutorAddress>>>,

    pending_packets_count: AtomicU64,

    changes_notifier_mutex: Mutex<()>,
    changes_notifier_condvar: Condvar,

    queues_allocator: ObjectsPool<ArrayQueue<(ExecutorAddress, PacketAny)>>,
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
            execution_managers_info: HashMap::new(),
            packets_map: Default::default(),
            full_executors: Default::default(),
            available_executors: Default::default(),
            duplicable_executors: Default::default(),
            holding_allocated: Default::default(),
            waiting_addresses: Default::default(),
            pending_packets_count: AtomicU64::new(0),
            changes_notifier_mutex: Default::default(),
            changes_notifier_condvar: Default::default(),
            queues_allocator: ObjectsPool::new(
                queue_buffers_pool_size,
                false,
                executor_buffer_capacity,
            ),
        }
    }

    pub fn set_output<P: ExecThreadPoolDataAddTrait + 'static>(
        &mut self,
        target_id: TypeId,
        output_id: TypeId,
        output_target: Arc<P>,
    ) {
        let mut exec_info = self
            .execution_managers_info
            .get(&target_id)
            .unwrap()
            .write();
        exec_info.output_type_id = output_id;
        exec_info.output_pool = Some(output_target);
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
                PoolAllocMode::Distinct { capacity } => PoolMode::Distinct {
                    pools_allocator: ObjectsPool::new(
                        executors_max_count,
                        false,
                        (capacity, true, pool_init_data),
                    ),
                },
            },
            executors_allocator: ObjectsPool::new(executors_max_count, false, ()),
        });

        let executor_info = Arc::new(RwLock::new(ExecutionManagerInfo {
            executor_type: E::EXECUTOR_TYPE,
            output_type_id: TypeId::of::<()>(),
            output_pool: None,
            allocator: None,
        }));

        let executor_info_aeu = Arc::downgrade(&executor_info);

        let allocate_execution_unit =
            move |addr: &ExecutorAddress, packet: &mut Option<PacketAny>| {
                let executor = executors_manager.executors_allocator.alloc_object();

                addr.executor_keeper.try_read().unwrap();

                if let Some(main_executor) = addr.executor_keeper.read().as_ref() {
                    return main_executor.downcast::<E>().clone_executor(executor);
                }

                let output_pool = executor_info_aeu
                    .upgrade()
                    .unwrap()
                    .read()
                    .output_pool
                    .clone();

                // TODO: Memory params
                let build_info = E::allocate_new_group(
                    global_params.clone(),
                    None,
                    packet.take().map(|p| p.downcast()),
                    |v| {
                        if v.len() > 0 {
                            output_pool.as_ref().unwrap().add_executors_batch(v);
                        }
                    },
                );

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
                            .as_ref()
                            .unwrap()
                            .add_data(addr, packet.upcast());
                    },
                ))
            };

        executor_info.write().allocator = Some(Box::new(allocate_execution_unit));

        let not_present = self
            .execution_managers_info
            .insert(TypeId::of::<E>(), executor_info)
            .is_none();

        self.waiting_addresses
            .insert(TypeId::of::<E>(), RwLock::new(SegQueue::new()));
        self.holding_allocated
            .insert(TypeId::of::<E>(), SegQueue::new());

        assert!(not_present);
    }

    pub fn register_executors_batch(&self, mut executors: Vec<ExecutorAddress>) {
        while executors.len() > 0 {
            let first_type = executors[0].executor_type_id;

            let exec_queue = self.waiting_addresses.get(&first_type).unwrap().write();
            executors
                .drain_filter(|e| e.executor_type_id == first_type)
                .for_each(|executor| {
                    let old_val = self
                        .packets_map
                        .insert(executor.to_weak(), self.queues_allocator.alloc_object());
                    assert!(old_val.is_none());
                    exec_queue.push(executor);
                });
        }
    }

    pub fn add_input_packet(&self, mut address: ExecutorAddress, mut packet: PacketAny) {
        self.pending_packets_count.fetch_add(1, Ordering::SeqCst);
        loop {
            match self
                .packets_map
                .get(&address.to_weak())
                .unwrap()
                .push((address, packet))
            {
                Ok(_) => {
                    self.changes_notifier_condvar.notify_all();
                    break;
                }
                Err(val) => {
                    address = val.0;
                    packet = val.1;
                }
            }
            let mut wait_lock = self.changes_notifier_mutex.lock();
            self.changes_notifier_condvar
                .wait_for(&mut wait_lock, Duration::from_millis(100));
        }
    }

    fn alloc_executor(&self, address: &ExecutorAddress) -> Option<GenericExecutor> {
        let executor_info = self
            .execution_managers_info
            .get(&address.executor_type_id)
            .unwrap()
            .read();

        let mut packet = match executor_info.executor_type {
            ExecutorType::SingleUnit | ExecutorType::MultipleUnits => None,
            ExecutorType::MultipleCommonPacketUnits => Some(self.get_packet_from_addr(address)?),
        };

        let executor = (executor_info.allocator.as_ref().unwrap())(address, &mut packet);

        if let Some(packet) = packet {
            self.add_input_packet(address.clone(), packet);
        }
        executor
    }

    fn get_packet_from_addr(&self, addr: &ExecutorAddress) -> Option<PacketAny> {
        match self.packets_map.get(&addr.to_weak()) {
            None => None,
            Some(packets) => {
                if let Some(packet) = packets.value().pop() {
                    Some(packet.1)
                } else {
                    None
                }
            }
        }
    }

    pub fn find_work(&self, last_executor: &mut Option<GenericExecutor>) -> Option<PacketAny> {
        if self.pending_packets_count.load(Ordering::SeqCst) == 0 {
            let mut wait_lock = self.changes_notifier_mutex.lock();
            self.changes_notifier_condvar
                .wait_for(&mut wait_lock, Duration::from_millis(100));
        }

        let mut new_executor: Option<(GenericExecutor, bool)> = None;

        'main_scheduling_loop: while self.pending_packets_count.load(Ordering::SeqCst) > 0 {
            if let Some((new_executor, is_new)) = new_executor.take() {
                if let Some(executor) = last_executor.take() {
                    let address = executor.get_address();
                    self.holding_allocated
                        .get(&address.executor_type_id)
                        .unwrap()
                        .push(Arc::downgrade(&executor));
                }
                if is_new {
                    new_executor.pre_execute();
                }
                *last_executor = Some(new_executor);

                // println!("Packets list:");
                // for pmap in self.packets_map.iter() {
                //     let addr = pmap.key();
                //     let value = pmap.deref();
                //     let value = value.deref();
                //     let value = value.deref();
                //
                //     println!("Address: {:?} => {:?}", addr, value.len());
                // }
            }

            if let Some(executor) = last_executor {
                let strong_addr = executor.get_address();

                if let Some(packet) = self.get_packet_from_addr(&strong_addr) {
                    self.pending_packets_count.fetch_sub(1, Ordering::Relaxed);
                    self.changes_notifier_condvar.notify_all();
                    return Some(packet);
                } else if executor.is_finished() {
                    last_executor.take();
                }
            }

            let mut duplicated_executor = None;

            while let Some(weak_addr) = self.duplicable_executors.pop() {
                if let Some(addr) = weak_addr.get_strong() {
                    if let Some(executor) = &last_executor {
                        if addr == executor.get_address() {
                            self.duplicable_executors.push(weak_addr);
                            if self.duplicable_executors.len() == 1 {
                                duplicated_executor = None;
                                break;
                            } else {
                                continue;
                            }
                        }
                    }

                    if let Some(executor) = self.alloc_executor(&addr) {
                        self.duplicable_executors.push(weak_addr);
                        duplicated_executor = Some(executor);
                        break;
                    }
                }
            }

            if let Some(executor) = duplicated_executor {
                new_executor = Some((executor, true));
                continue;
            }

            for (_, addr_queue) in self.waiting_addresses.iter() {
                let addr_queue = addr_queue.read();
                if let Some(addr) = addr_queue.pop() {
                    let executor = self.alloc_executor(&addr).unwrap();

                    if executor.can_split() {
                        self.duplicable_executors.push(addr.to_weak());
                        self.changes_notifier_condvar.notify_all();
                    }
                    new_executor = Some((executor, true));
                    continue 'main_scheduling_loop;
                }
            }

            for (_, wref_queue) in self.holding_allocated.iter() {
                let mut allowed_iterations = wref_queue.len();
                while let Some(weak_ref) = wref_queue.pop() {
                    if let Some(executor) = weak_ref.upgrade() {
                        if !executor.is_finished() {
                            let has_packets = self
                                .packets_map
                                .get(&executor.get_address().to_weak())
                                .as_ref()
                                .unwrap()
                                .len()
                                > 0;

                            if has_packets {
                                new_executor = Some((executor, false));
                                continue 'main_scheduling_loop;
                            } else {
                                wref_queue.push(weak_ref);
                            }
                        }
                    }
                    if allowed_iterations == 0 {
                        break;
                    }
                    allowed_iterations -= 1;
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
