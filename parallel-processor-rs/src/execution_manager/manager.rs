use crate::execution_manager::executor::{Executor, Packet, PacketsPool};
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::objects_pool::PoolObject;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub type GenericExecutor<I, O> = Arc<dyn ExecutionManagerTrait<InputPacket = I, OutputPacket = O>>;

pub trait ExecutionManagerTrait: Send + Sync {
    type InputPacket: Send + Sync;
    type OutputPacket: Send + Sync;
    fn process_packet(
        &self,
        context: *mut (),
        packet: Packet<Self::InputPacket>,
        output_fn: fn(*mut (), ExecutorAddress, Packet<Self::OutputPacket>),
    );

    fn get_address(&self) -> ExecutorAddress;

    fn can_split(&self) -> bool;
}

pub struct ExecutionManager<E: Executor> {
    executor: Mutex<PoolObject<E>>,
    weak_address: RwLock<WeakExecutorAddress>,
    pool: Option<Arc<PoolObject<PacketsPool<E::OutputPacket>>>>,
    build_info: Option<(E::BuildParams, AtomicUsize, usize)>,
}

impl<E: Executor + 'static> ExecutionManager<E> {
    pub fn new(
        mut executor: PoolObject<E>,
        build_info: E::BuildParams,
        pool: Option<Arc<PoolObject<PacketsPool<E::OutputPacket>>>>,
        address: ExecutorAddress,
    ) -> GenericExecutor<E::InputPacket, E::OutputPacket> {
        executor.get_value_mut().reinitialize(&build_info, || {
            pool.as_ref().unwrap().get_value().alloc_object()
        });

        let maximum_instances = executor.get_value().get_maximum_concurrency();

        let mut self_ = Arc::new(Self {
            executor: Mutex::new(executor),
            weak_address: RwLock::new(WeakExecutorAddress::empty()),
            pool,
            build_info: Some((build_info, AtomicUsize::new(0), maximum_instances)),
        });
        *address.executor_keeper.write() = Some(self_.clone());
        *self_.weak_address.write() = address.to_weak();

        self_
    }

    pub fn clone_executor(
        &self,
        mut new_core: PoolObject<E>,
    ) -> Option<GenericExecutor<E::InputPacket, E::OutputPacket>> {
        let (build_info, current_count, max_count) = self.build_info.as_ref().unwrap();

        if current_count.fetch_add(1, Ordering::Relaxed) >= *max_count {
            return None;
        }

        new_core.get_value_mut().reinitialize(build_info, || {
            self.pool.as_ref().unwrap().get_value().alloc_object()
        });

        Some(Arc::new(Self {
            executor: Mutex::new(new_core),
            weak_address: RwLock::new(self.weak_address.read().clone()),
            pool: self.pool.clone(),
            build_info: None,
        }))
    }
}

impl<E: Executor> ExecutionManagerTrait for ExecutionManager<E> {
    type InputPacket = E::InputPacket;
    type OutputPacket = E::OutputPacket;

    fn process_packet(
        &self,
        context: *mut (),
        packet: Packet<Self::InputPacket>,
        output_fn: fn(*mut (), ExecutorAddress, Packet<Self::OutputPacket>),
    ) {
        let mut executor = self.executor.lock();
        executor.get_value_mut().execute(
            packet,
            || self.pool.as_ref().unwrap().get_value().alloc_object(),
            |addr, packet| output_fn(context, addr, packet),
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
