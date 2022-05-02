use crate::execution_manager::executor::Executor;
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::objects_pool::ObjectsPool;
use crate::execution_manager::objects_pool::PoolObject;
use crate::execution_manager::packet::{Packet, PacketAny, PacketsPool};
use parking_lot::{Mutex, RwLock};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub type GenericExecutor = Arc<dyn ExecutionManagerTrait>;

pub trait ExecutionManagerTrait: Send + Sync {
    fn process_packet(&self, packet: PacketAny);

    fn get_address(&self) -> ExecutorAddress;

    fn can_split(&self) -> bool;
}

pub struct ExecutionManager<E: Executor> {
    executor: Mutex<PoolObject<E>>,
    weak_address: RwLock<WeakExecutorAddress>,
    pool: Option<Arc<PoolObject<PacketsPool<E::OutputPacket>>>>,
    build_info: Option<(E::BuildParams, AtomicUsize, usize)>,
    output_fn: Arc<dyn (Fn(ExecutorAddress, Packet<E::OutputPacket>)) + Sync + Send>,
}

static EXECUTORS_COUNT: AtomicU64 = AtomicU64::new(0);

impl<E: Executor + 'static> ExecutionManager<E> {
    pub fn new(
        mut executor: PoolObject<E>,
        build_info: E::BuildParams,
        pool: Option<Arc<PoolObject<PacketsPool<E::OutputPacket>>>>,
        address: ExecutorAddress,
        output_fn: impl Fn(ExecutorAddress, Packet<E::OutputPacket>) + Sync + Send + 'static,
    ) -> GenericExecutor {
        executor.reinitialize(&build_info, || pool.as_ref().unwrap().alloc_packet());
        executor.pre_execute(
            || pool.as_ref().unwrap().alloc_packet(),
            |addr, packet| output_fn(addr, packet),
        );

        let maximum_instances = executor.get_maximum_concurrency();
        EXECUTORS_COUNT.fetch_add(1, Ordering::Relaxed);

        let mut self_ = Arc::new(Self {
            executor: Mutex::new(executor),
            weak_address: RwLock::new(WeakExecutorAddress::empty()),
            pool,
            build_info: Some((build_info, AtomicUsize::new(1), maximum_instances)),
            output_fn: Arc::new(output_fn),
        });
        *address.executor_keeper.write() = Some(self_.clone());
        *self_.weak_address.write() = address.to_weak();

        self_
    }

    pub fn clone_executor(&self, mut new_core: PoolObject<E>) -> Option<GenericExecutor> {
        let (build_info, current_count, max_count) = self.build_info.as_ref().unwrap();

        if current_count.fetch_add(1, Ordering::Relaxed) >= *max_count {
            return None;
        }
        EXECUTORS_COUNT.fetch_add(1, Ordering::Relaxed);

        new_core.reinitialize(build_info, || self.pool.as_ref().unwrap().alloc_packet());
        new_core.pre_execute(
            || self.pool.as_ref().unwrap().alloc_packet(),
            |addr, packet| (self.output_fn)(addr, packet),
        );

        Some(Arc::new(Self {
            executor: Mutex::new(new_core),
            weak_address: RwLock::new(self.weak_address.read().clone()),
            pool: self.pool.clone(),
            build_info: None,
            output_fn: self.output_fn.clone(),
        }))
    }
}

impl<E: Executor> ExecutionManagerTrait for ExecutionManager<E> {
    fn process_packet(&self, packet: PacketAny) {
        let mut executor = self.executor.lock();
        executor.execute(
            packet.downcast(),
            || self.pool.as_ref().unwrap().alloc_packet(),
            self.output_fn.deref(),
        );
    }

    fn get_address(&self) -> ExecutorAddress {
        self.weak_address.read().get_strong().unwrap()
    }

    fn can_split(&self) -> bool {
        self.build_info
            .as_ref()
            .map(|bi| bi.1.load(Ordering::Relaxed) < bi.2)
            .unwrap_or(false)
    }
}

impl<E: Executor> Drop for ExecutionManager<E> {
    fn drop(&mut self) {
        let index = EXECUTORS_COUNT.fetch_sub(1, Ordering::Relaxed);

        self.executor.lock().finalize(self.output_fn.deref())
    }
}
