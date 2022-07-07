use crate::execution_manager::async_channel::AsyncChannel;
use crate::execution_manager::execution_context::{
    ExecutionContext, ExecutorDropper, PacketsChannel,
};
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::memory_tracker::MemoryTracker;
use crate::execution_manager::objects_pool::{PoolObject, PoolObjectTrait};
use crate::execution_manager::packet::{Packet, PacketAny};
use crate::execution_manager::packet::{PacketTrait, PacketsPool};
use parking_lot::Mutex;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::runtime::Handle;

static EXECUTOR_GLOBAL_ID: AtomicU64 = AtomicU64::new(0);

pub trait AsyncExecutor: Sized + Send + Sync + 'static {
    type InputPacket: Send + Sync + 'static;
    type OutputPacket: PacketTrait + Send + Sync + 'static;
    type GlobalParams: Send + Sync + 'static;

    type AsyncExecutorFuture<'a>: Future<Output = ()> + Send + 'a;

    fn generate_new_address() -> ExecutorAddress {
        let exec = ExecutorAddress {
            executor_keeper: Arc::new(ExecutorDropper::new()),
            executor_type_id: std::any::TypeId::of::<Self>(),
            executor_internal_id: EXECUTOR_GLOBAL_ID.fetch_add(1, Ordering::Relaxed),
        };
        exec
    }

    fn new() -> Self;

    fn async_executor_main<'a>(
        &'a mut self,
        global_params: &'a Self::GlobalParams,
        receiver: ExecutorReceiver<Self>,
        memory_tracker: MemoryTracker<Self>,
    ) -> Self::AsyncExecutorFuture<'a>;
}

pub struct ExecutorReceiver<E: AsyncExecutor> {
    pub(crate) context: Arc<ExecutionContext>,
    pub(crate) addresses_channel: AsyncChannel<(
        WeakExecutorAddress,
        Arc<AtomicU64>,
        Arc<PoolObject<PacketsChannel>>,
    )>,
    pub(crate) _phantom: PhantomData<E>,
}

impl<E: AsyncExecutor> ExecutorReceiver<E> {
    pub async fn obtain_address(&mut self) -> Result<ExecutorAddressOperations<E>, ()> {
        let (addr, counter, channel) = self.addresses_channel.recv().await?;

        Ok(ExecutorAddressOperations {
            addr,
            counter,
            channel,
            context: self.context.clone(),
            packets_pool: self.context.allocate_pool::<E>(),
            is_finished: AtomicBool::new(false),
            _phantom: PhantomData,
        })
    }
}

pub struct ExecutorAddressOperations<'a, E: AsyncExecutor> {
    addr: WeakExecutorAddress,
    counter: Arc<AtomicU64>,
    channel: Arc<PoolObject<PacketsChannel>>,
    context: Arc<ExecutionContext>,
    packets_pool: Option<Arc<PoolObject<PacketsPool<E::OutputPacket>>>>,
    is_finished: AtomicBool,
    _phantom: PhantomData<&'a E>,
}
impl<'a, E: AsyncExecutor> ExecutorAddressOperations<'a, E> {
    pub async fn receive_packet(&self) -> Option<Packet<E::InputPacket>> {
        if self.is_finished.load(Ordering::SeqCst) {
            return None;
        }

        match self.channel.recv().await {
            Ok(packet) => Some(packet.downcast()),
            Err(()) => {
                self.is_finished.store(true, Ordering::SeqCst);
                None
            }
        }
    }
    pub fn declare_addresses(&self, addresses: Vec<ExecutorAddress>) {
        self.context.register_executors_batch(addresses);
    }
    pub async fn packet_alloc(&self) -> Packet<E::OutputPacket> {
        self.packets_pool.as_ref().unwrap().alloc_packet().await
    }
    pub fn packet_alloc_blocking(&self) -> Packet<E::OutputPacket> {
        self.packets_pool.as_ref().unwrap().alloc_packet_blocking()
    }
    pub fn packet_send(&self, address: ExecutorAddress, packet: Packet<E::OutputPacket>) {
        self.context.send_packet(address, packet);
    }

    pub fn make_spawner(&self) -> ExecutorsSpawner<'a> {
        ExecutorsSpawner {
            handles: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

impl<'a, E: AsyncExecutor> Drop for ExecutorAddressOperations<'a, E> {
    fn drop(&mut self) {
        if self.counter.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.context.wait_condvar.notify_all();
        }
    }
}

pub struct ExecutorsSpawner<'a> {
    handles: Vec<tokio::task::JoinHandle<()>>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> ExecutorsSpawner<'a> {
    pub fn spawn_executor(&mut self, executor: impl Future<Output = ()> + 'a) {
        let current_runtime = Handle::current();
        let executor = unsafe {
            std::mem::transmute::<_, Pin<Box<dyn Future<Output = ()> + Send>>>(
                Box::pin(executor) as Pin<Box<dyn Future<Output = ()>>>
            )
        };
        self.handles.push(current_runtime.spawn(executor));
    }

    pub async fn executors_await(&mut self) {
        for handle in self.handles.drain(..) {
            handle.await;
        }
    }
}

impl<'a> Drop for ExecutorsSpawner<'a> {
    fn drop(&mut self) {
        if self.handles.len() > 0 {
            panic!("Executors not awaited!");
        }
    }
}

//
// pub trait Executor:
//     Sized + PoolObjectTrait<InitData = (Arc<Self::GlobalParams>, MemoryTracker<Self>)> + Sync + Send
// {
//     const EXECUTOR_TYPE: ExecutorType;
//
//     const MEMORY_FIELDS_COUNT: usize;
//     const MEMORY_FIELDS: &'static [&'static str];
//
//     const BASE_PRIORITY: u64;
//     const PACKET_PRIORITY_MULTIPLIER: u64;
//     const STRICT_POOL_ALLOC: bool;
//
//     type InputPacket: Send + Sync;
//     type OutputPacket: Send + Sync + PacketTrait;
//     type GlobalParams: Send + Sync;
//     type MemoryParams: Send + Sync;
//     type BuildParams: Send + Sync + Clone;
//
//
//     fn allocate_new_group<E: ExecutorOperations<Self>>(
//         global_params: Arc<Self::GlobalParams>,
//         memory_params: Option<Self::MemoryParams>,
//         common_packet: Option<Packet<Self::InputPacket>>,
//         ops: E,
//     ) -> (Self::BuildParams, usize);
//
//     fn required_pool_items(&self) -> u64;
//
//     fn pre_execute<E: ExecutorOperations<Self>>(
//         &mut self,
//         reinit_params: Self::BuildParams,
//         ops: E,
//     );
//     fn execute<E: ExecutorOperations<Self>>(
//         &mut self,
//         input_packet: Packet<Self::InputPacket>,
//         ops: E,
//     );
//     fn finalize<E: ExecutorOperations<Self>>(&mut self, ops: E);
//
//     fn is_finished(&self) -> bool;
//
//     fn get_current_memory_params(&self) -> Self::MemoryParams;
// }

// impl AsyncExecutor {
//     type PacketType;
//
//     fn executor_main(receiver: ExecutorReceiver) {
//         while let Ok(address) = receiver.get_address().await {
//             allocate_new_group();
//
//             receiver.spawn_tasks(|s| {
//                 for _ in 0..max_concurrency {
//                     s.spawn(|| {
//                         // DO_WORK
//
//                         while address.get_packet() {
//                             // Process packet
//
//                             receiver.update_memory_params();
//                         }
//                         address.send_packet(packet);
//
//                         finalize();
//                     })
//                 }
//             });
//         }
//     }
// }
