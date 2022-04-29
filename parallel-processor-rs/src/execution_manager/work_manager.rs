use crate::execution_manager::executor::{Executor, Packet, PacketTrait, PacketsPool};
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::executors_list::{ExecutorAllocMode, ExecutorsList, PoolAllocMode};
use crate::execution_manager::manager::{ExecutionManager, ExecutionManagerTrait, GenericExecutor};
use crate::execution_manager::objects_pool::{ObjectsPool, PoolObject, PoolObjectTrait};
use crossbeam::queue::{ArrayQueue, SegQueue};
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Weak};

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

pub struct WorkManager<I: Send + Sync + 'static, O: Send + Sync + PoolObjectTrait> {
    allocator_functions: HashMap<
        u64,
        Box<dyn (Fn(&ExecutorAddress) -> Option<GenericExecutor<I, O>>) + Sync + Send>,
    >,
    packets_map: DashMap<ExecutorAddress, PoolObject<ArrayQueue<Packet<I>>>>,

    full_executors: SegQueue<GenericExecutor<I, O>>,
    available_executors: SegQueue<GenericExecutor<I, O>>,
    duplicable_executors: SegQueue<WeakExecutorAddress>,

    waiting_addresses: SegQueue<ExecutorAddress>,

    changes_notifier_mutex: Mutex<()>,
    changes_notifier_condvar: Condvar,

    queues_allocator: ObjectsPool<ArrayQueue<Packet<I>>>,
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

impl<I: Send + Sync + 'static, O: Send + Sync + PoolObjectTrait> WorkManager<I, O> {
    pub fn new(queue_buffers_pool_size: usize, executor_buffer_capacity: usize) -> Self {
        Self {
            allocator_functions: HashMap::new(),
            packets_map: Default::default(),
            full_executors: Default::default(),
            available_executors: Default::default(),
            duplicable_executors: Default::default(),
            waiting_addresses: Default::default(),
            changes_notifier_mutex: Default::default(),
            changes_notifier_condvar: Default::default(),
            queues_allocator: ObjectsPool::new(
                queue_buffers_pool_size,
                false,
                executor_buffer_capacity,
            ),
        }
    }

    pub fn add_executors<E: Executor<InputPacket = I, OutputPacket = O>>(
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
                        capacity,
                        false,
                        (executors_max_count, true, pool_init_data),
                    ),
                },
            },
            executors_allocator: ObjectsPool::new(executors_max_count, false, ()),
        });

        let allocate_execution_unit = move |addr: &ExecutorAddress| {
            let executor = executors_manager.executors_allocator.alloc_object();

            addr.executor_keeper.try_read().unwrap();

            if let Some(main_executor) = addr.executor_keeper.read().as_ref() {
                println!("Cloning!");
                return main_executor
                    .downcast_ref::<ExecutionManager<E>>()
                    .unwrap()
                    .clone_executor(executor);
            }

            // TODO: Memory params
            let build_info = E::allocate_new_group(global_params.clone(), None);
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
            ))
        };

        let not_present = self
            .allocator_functions
            .insert(E::EXECUTOR_TYPE_INDEX, Box::new(allocate_execution_unit))
            .is_none();
        assert!(not_present);
    }

    pub fn add_input_packet(&self, address: ExecutorAddress, mut packet: Packet<I>) {
        loop {
            match self
                .packets_map
                .entry(address.clone())
                .or_insert_with(|| {
                    println!("Add waiting address!");
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

    fn alloc_executor(&self, address: &ExecutorAddress) -> Option<GenericExecutor<I, O>> {
        (self
            .allocator_functions
            .get(&address.executor_type_id)
            .unwrap())(address)
    }

    fn get_packet_from_addr(&self, addr: &ExecutorAddress) -> Option<Packet<I>> {
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

    pub fn find_work(
        &self,
        mut last_executor: Option<GenericExecutor<I, O>>,
    ) -> Option<(Packet<I>, GenericExecutor<I, O>)> {
        println!("Finding work...");
        loop {
            if let Some(executor) = last_executor {
                println!("More work...");
                let strong_addr = executor.get_address();
                if let Some(packet) = self.get_packet_from_addr(&strong_addr) {
                    println!("Found work...");
                    return Some((packet, executor));
                } else {
                    println!("No packets found for current!");
                    last_executor = Some(executor);
                }
            }

            while let Some(weak_addr) = self.duplicable_executors.pop() {
                println!("Duplicate work...");
                if let Some(addr) = weak_addr.get_strong() {
                    if let Some(executor) = self.alloc_executor(&addr) {
                        if let Some(packet) = self.get_packet_from_addr(&addr) {
                            self.duplicable_executors.push(weak_addr);
                            self.changes_notifier_condvar.notify_all();
                            println!("Found work...");
                            return Some((packet, executor));
                        }
                    }
                }
            }

            if let Some(addr) = self.waiting_addresses.pop() {
                println!("Waiting work...");
                let executor = self.alloc_executor(&addr).unwrap();
                println!("Allocated executor!");

                if executor.can_split() {
                    self.duplicable_executors.push(addr.to_weak());
                    self.changes_notifier_condvar.notify_all();
                }

                if let Some(packet) = self.get_packet_from_addr(&addr) {
                    println!("Found work...");
                    return Some((packet, executor));
                }
            }

            println!("Waiting for news...");
            let mut wait_lock = self.changes_notifier_mutex.lock();
            self.changes_notifier_condvar.wait(&mut wait_lock);
            println!("News!");
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
        println!("Returned None!");
        None
    }
}
