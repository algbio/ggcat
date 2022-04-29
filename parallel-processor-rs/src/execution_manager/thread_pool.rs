use crate::execution_manager::packet::Packet;
use crate::execution_manager::work_manager::WorkManager;
use parking_lot::RwLock;
use std::marker::PhantomData;
use std::sync::Arc;

pub trait ExecThreadPoolDataAddTrait {
    type InputPacket;
    fn add_data(&self, packet: Packet<Self::InputPacket>);
}

pub struct ExecThreadPool<I, O> {
    pub(crate) work_manager: RwLock<WorkManager<I, O>>,
}

impl<I, O> ExecThreadPool<I, O> {
    pub fn new(threads_count: usize) -> Arc<Self> {
        Arc::new(Self {
            work_manager: RwLock::new(WorkManager::new()),
        })
    }

    pub fn set_output<X>(&mut self, target: &ExecThreadPool<O, X>) {}

    fn thread(&self) {
        // while let Some(work) = self.dispatch() {
        //     work.execute();
        // }
    }

    fn start() {}
}

impl<I, O> ExecThreadPoolDataAddTrait for ExecThreadPool<I, O> {
    type InputPacket = I;

    fn add_data(&self, packet: Packet<Self::InputPacket>) {
        todo!()
    }
}
