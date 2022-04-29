use crate::execution_manager::executor::Packet;
use crate::execution_manager::executor_address::ExecutorAddress;
use crate::execution_manager::objects_pool::PoolObjectTrait;
use crate::execution_manager::work_manager::WorkManager;
use parking_lot::{Mutex, RwLock};
use std::marker::PhantomData;
use std::sync::Arc;
use std::thread::JoinHandle;

pub trait ExecThreadPoolDataAddTrait: Send + Sync {
    type InputPacket;
    fn add_data(&self, addr: ExecutorAddress, packet: Packet<Self::InputPacket>);
}

pub struct ExecThreadPool<I: Send + Sync + 'static, O: Send + Sync + PoolObjectTrait> {
    pub(crate) work_manager: RwLock<WorkManager<I, O>>,
    output_thread_pool: RwLock<Option<Arc<dyn ExecThreadPoolDataAddTrait<InputPacket = O>>>>,
    threads_count: usize,
    thread_handles: Mutex<Vec<JoinHandle<()>>>,
}

impl<I: Send + Sync + 'static, O: Send + Sync + PoolObjectTrait> ExecThreadPool<I, O> {
    pub fn new(threads_count: usize, executors_buffer_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            work_manager: RwLock::new(WorkManager::new(
                threads_count * 2,
                executors_buffer_capacity,
            )),
            output_thread_pool: RwLock::new(None),
            threads_count,
            thread_handles: Mutex::new(Vec::new()),
        })
    }

    pub fn set_output<X: Send + Sync + PoolObjectTrait>(&self, target: &Arc<ExecThreadPool<O, X>>) {
        *self.output_thread_pool.write() = Some(target.clone());
    }

    fn thread(&self) {
        let work_manager = self.work_manager.read();
        let mut executor = None;

        let mut self_ref = &mut (self,);
        while let Some((packet, new_exec)) = work_manager.find_work(executor) {
            new_exec.process_packet(
                self_ref as *mut _ as *mut (),
                packet,
                |self_ptr_, addr, packet| {
                    let self_ = unsafe { &*(self_ptr_ as *mut (&Self,)) };
                    self_
                        .0
                        .output_thread_pool
                        .read()
                        .as_ref()
                        .expect("Needed another pool to send messages!")
                        .add_data(addr, packet);
                },
            );

            executor = Some(new_exec);
        }
    }

    pub fn start(self: &Arc<Self>) {
        let mut handles = self.thread_handles.lock();
        assert_eq!(handles.len(), 0);
        for _ in 0..self.threads_count {
            let self_ = self.clone();
            handles.push(std::thread::spawn(move || {
                self_.thread();
            }));
        }
    }

    pub fn join(&self) {
        let mut handles = self.thread_handles.lock();
        for handle in handles.drain(..) {
            handle.join().unwrap();
        }
    }
}

impl<I: Send + Sync + 'static, O: Send + Sync + PoolObjectTrait> ExecThreadPoolDataAddTrait
    for ExecThreadPool<I, O>
{
    type InputPacket = I;

    fn add_data(&self, addr: ExecutorAddress, packet: Packet<Self::InputPacket>) {
        self.work_manager.read().add_input_packet(addr, packet);
    }
}

impl<I: Send + Sync + 'static, O: Send + Sync + PoolObjectTrait> Drop for ExecThreadPool<I, O> {
    fn drop(&mut self) {
        self.join();
    }
}
