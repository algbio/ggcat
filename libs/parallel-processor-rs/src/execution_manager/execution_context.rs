use crate::execution_manager::async_channel::{AsyncChannel, DoublePriorityAsyncChannel};
use crate::execution_manager::executor::AsyncExecutor;
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::memory_tracker::MemoryTrackerManager;
use crate::execution_manager::objects_pool::{ObjectsPool, PoolObject, PoolObjectTrait};
use crate::execution_manager::packet::{Packet, PacketAny, PacketTrait, PacketsPool};
use crate::execution_manager::thread_pool::ExecutorsHandle;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use parking_lot::{Condvar, Mutex};
use std::any::{Any, TypeId};
use std::cell::UnsafeCell;
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::Semaphore;

const ADDRESSES_BUFFER_SIZE: usize = 1;

pub enum PoolAllocMode {
    None,
    Shared { capacity: usize },
    Distinct { capacity: usize },
}

pub(crate) enum PacketsPoolStrategy<E: AsyncExecutor> {
    None,
    Shared(Arc<PoolObject<PacketsPool<E::OutputPacket>>>),
    Distinct {
        pools_allocator: ObjectsPool<PacketsPool<E::OutputPacket>>,
    },
}

pub struct ExecutorDropper {
    weak_addr: UnsafeCell<WeakExecutorAddress>,
    context: UnsafeCell<Weak<ExecutionContext>>,
}

impl ExecutorDropper {
    pub fn new() -> Self {
        Self {
            weak_addr: UnsafeCell::new(WeakExecutorAddress::empty()),
            context: UnsafeCell::new(Weak::new()),
        }
    }
}

impl Drop for ExecutorDropper {
    fn drop(&mut self) {
        let scheduler = unsafe { &*(self.context.get()) };
        if let Some(context) = scheduler.upgrade() {
            let address = unsafe { &*(self.weak_addr.get()) };
            context.dealloc_address(address.clone());
        }
    }
}

pub(crate) type PacketsChannel = AsyncChannel<PacketAny>;
impl PoolObjectTrait for PacketsChannel {
    type InitData = usize;

    fn allocate_new(size: &Self::InitData) -> Self {
        Self::new(*size)
    }

    fn reset(&mut self) {
        if self.try_recv().is_some() {
            panic!("Packets channel not empty!");
        }
        self.reopen();
    }
}

pub struct ExecutionContext {
    queues_pool: ObjectsPool<PacketsChannel>,
    pub(crate) waiting_addresses: Mutex<
        HashMap<
            TypeId,
            DoublePriorityAsyncChannel<(
                WeakExecutorAddress,
                Arc<AtomicU64>,
                Arc<PoolObject<PacketsChannel>>,
                Arc<dyn Any + Sync + Send + 'static>,
            )>,
        >,
    >,
    pub(crate) active_executors_counters: DashMap<TypeId, Arc<AtomicU64>>,
    pub(crate) addresses_map: DashMap<WeakExecutorAddress, Arc<PoolObject<PacketsChannel>>>,
    pub(crate) packet_pools: DashMap<TypeId, Box<dyn Any + Sync + Send>>,
    pub(crate) memory_tracker: Arc<MemoryTrackerManager>,
    pub(crate) start_semaphore: Semaphore,
    wait_mutex: Mutex<()>,
    pub(crate) wait_condvar: Condvar,
}

const MAX_SEMAPHORE_PERMITS: u32 = u32::MAX >> 3;

impl ExecutionContext {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            queues_pool: ObjectsPool::new(ADDRESSES_BUFFER_SIZE, 0),
            waiting_addresses: Mutex::new(HashMap::new()),
            active_executors_counters: DashMap::new(),
            addresses_map: DashMap::new(),
            packet_pools: DashMap::new(),
            memory_tracker: Arc::new(MemoryTrackerManager::new()),
            start_semaphore: Semaphore::new(0),
            wait_mutex: Mutex::new(()),
            wait_condvar: Condvar::new(),
        })
    }

    pub fn register_executor_type<E: AsyncExecutor>(
        &self,
        executors_max_count: usize,
        pool_alloc_mode: PoolAllocMode,
        pool_init_data: <E::OutputPacket as PoolObjectTrait>::InitData,
    ) {
        self.active_executors_counters
            .insert(TypeId::of::<E>(), Arc::new(AtomicU64::new(0)));
        self.waiting_addresses
            .lock()
            .insert(TypeId::of::<E>(), DoublePriorityAsyncChannel::new(0));
        self.packet_pools.insert(
            TypeId::of::<E>(),
            Box::new(match pool_alloc_mode {
                PoolAllocMode::None => PacketsPoolStrategy::<E>::None,
                PoolAllocMode::Shared { capacity } => {
                    PacketsPoolStrategy::<E>::Shared(Arc::new(PoolObject::new_simple(
                        PacketsPool::new(capacity, pool_init_data, &self.memory_tracker),
                    )))
                }
                PoolAllocMode::Distinct { capacity } => PacketsPoolStrategy::<E>::Distinct {
                    pools_allocator: ObjectsPool::new(
                        executors_max_count,
                        (capacity, pool_init_data, self.memory_tracker.clone()),
                    ),
                },
            }),
        );
    }

    pub fn get_allocated_executors(&self, executor_type_id: &TypeId) -> u64 {
        self.active_executors_counters
            .get(executor_type_id)
            .unwrap()
            .load(Ordering::SeqCst)
    }

    pub fn register_executors_batch(
        self: &Arc<Self>,
        executors: Vec<ExecutorAddress>,
        priority: usize,
    ) {
        let mut waiting_addresses = self.waiting_addresses.lock();

        for executor in executors {
            unsafe {
                *(executor.executor_keeper.context.get()) = Arc::downgrade(self);
                *(executor.executor_keeper.weak_addr.get()) = executor.to_weak();
            }

            let queue = Arc::new(self.queues_pool.alloc_object_force());

            let old_val = self.addresses_map.insert(executor.to_weak(), queue.clone());

            let counter = self
                .active_executors_counters
                .get(&executor.executor_type_id)
                .unwrap()
                .clone();

            counter.fetch_add(1, Ordering::SeqCst);
            assert!(old_val.is_none());

            waiting_addresses
                .get_mut(&executor.executor_type_id)
                .unwrap()
                .send_with_priority(
                    (executor.to_weak(), counter, queue, executor.init_data),
                    priority,
                    false,
                );
        }
    }

    pub(crate) fn add_input_packet(&self, addr: ExecutorAddress, packet: PacketAny) {
        self.addresses_map
            .get(&addr.to_weak())
            .unwrap()
            .send(packet, false);
    }

    pub(crate) fn send_packet<T: PacketTrait>(&self, addr: ExecutorAddress, packet: Packet<T>) {
        self.memory_tracker.add_queue_packet(packet.deref());
        self.addresses_map
            .get(&addr.to_weak())
            .unwrap()
            .send(packet.upcast(), false);
    }

    pub(crate) async fn allocate_pool<E: AsyncExecutor>(
        &self,
        force: bool,
    ) -> Option<Arc<PoolObject<PacketsPool<E::OutputPacket>>>> {
        match self
            .packet_pools
            .get(&TypeId::of::<E>())
            .unwrap()
            .downcast_ref::<PacketsPoolStrategy<E>>()
            .unwrap()
            .deref()
        {
            PacketsPoolStrategy::None => None,
            PacketsPoolStrategy::Shared(pool) => Some(pool.clone()),
            PacketsPoolStrategy::Distinct { pools_allocator } => Some(Arc::new(if force {
                pools_allocator.alloc_object_force()
            } else {
                pools_allocator.alloc_object().await
            })),
        }
    }

    fn dealloc_address(&self, addr: WeakExecutorAddress) {
        let channel = self.addresses_map.remove(&addr).unwrap();
        channel.1.release();
    }

    pub fn start(&self) {
        self.start_semaphore
            .add_permits(MAX_SEMAPHORE_PERMITS as usize);
    }

    pub fn wait_for_completion<E: AsyncExecutor>(&self, _handle: ExecutorsHandle<E>) {
        let mut wait_mutex = self.wait_mutex.lock();
        let counter = self
            .active_executors_counters
            .get(&TypeId::of::<E>())
            .unwrap()
            .value()
            .clone();
        loop {
            // println!(
            //     "Waiting for {} {}",
            //     std::any::type_name::<E>(),
            //     counter.load(Ordering::Relaxed)
            // );
            if counter.load(Ordering::Relaxed) == 0 {
                return;
            }
            self.wait_condvar
                .wait_for(&mut wait_mutex, Duration::from_millis(100));
        }
    }

    pub fn get_pending_executors_count<E: AsyncExecutor>(
        &self,
        _handle: ExecutorsHandle<E>,
    ) -> u64 {
        self.get_allocated_executors(&TypeId::of::<E>())
    }

    pub fn join_all(&self) {
        let addresses = self.waiting_addresses.lock();
        addresses.iter().for_each(|addr| addr.1.release());
        drop(addresses);

        let mut wait_mutex = self.wait_mutex.lock();
        loop {
            self.wait_condvar
                .wait_for(&mut wait_mutex, Duration::from_millis(100));
            if self
                .start_semaphore
                .try_acquire_many(MAX_SEMAPHORE_PERMITS)
                .is_ok()
            {
                break;
            }
        }
    }
}
