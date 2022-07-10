use crate::execution_manager::async_channel::DoublePriorityAsyncChannel;
use crate::execution_manager::execution_context::{
    ExecutionContext, ExecutorDropper, PacketsChannel,
};
use crate::execution_manager::executor_address::{ExecutorAddress, WeakExecutorAddress};
use crate::execution_manager::memory_tracker::MemoryTracker;
use crate::execution_manager::objects_pool::PoolObject;
use crate::execution_manager::packet::{Packet, PacketTrait, PacketsPool};
use std::any::Any;
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
    type InitData: Send + Sync + Clone + 'static;

    type AsyncExecutorFuture<'a>: Future<Output = ()> + Send + 'a;

    fn generate_new_address(data: Self::InitData) -> ExecutorAddress {
        let exec = ExecutorAddress {
            executor_keeper: Arc::new(ExecutorDropper::new()),
            init_data: Arc::new(data),
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
    pub(crate) addresses_channel: DoublePriorityAsyncChannel<(
        WeakExecutorAddress,
        Arc<AtomicU64>,
        Arc<PoolObject<PacketsChannel>>,
        Arc<dyn Any + Sync + Send + 'static>,
    )>,
    pub(crate) _phantom: PhantomData<E>,
}

impl<E: AsyncExecutor> ExecutorReceiver<E> {
    pub async fn obtain_address(
        &mut self,
    ) -> Result<(ExecutorAddressOperations<E>, Arc<E::InitData>), ()> {
        self.obtain_address_with_priority(0).await
    }

    pub async fn obtain_address_with_priority(
        &mut self,
        priority: usize,
    ) -> Result<(ExecutorAddressOperations<E>, Arc<E::InitData>), ()> {
        let (addr, counter, channel, init_data) =
            self.addresses_channel.recv_offset(priority).await?;

        Ok((
            ExecutorAddressOperations {
                addr,
                counter,
                channel,
                context: self.context.clone(),
                is_finished: AtomicBool::new(false),
                _phantom: PhantomData,
            },
            init_data.downcast().unwrap(),
        ))
    }
}

pub struct ExecutorAddressOperations<'a, E: AsyncExecutor> {
    addr: WeakExecutorAddress,
    counter: Arc<AtomicU64>,
    channel: Arc<PoolObject<PacketsChannel>>,
    context: Arc<ExecutionContext>,
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
    pub fn declare_addresses(&self, addresses: Vec<ExecutorAddress>, priority: usize) {
        self.context.register_executors_batch(addresses, priority);
    }
    pub async fn pool_alloc_await(&self) -> Arc<PoolObject<PacketsPool<E::OutputPacket>>> {
        self.context.allocate_pool::<E>(false).await.unwrap()
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

    pub fn get_address(&self) -> WeakExecutorAddress {
        self.addr
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
            handle.await.unwrap();
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
