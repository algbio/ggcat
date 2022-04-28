use crate::execution_manager::packet::Packet;
use std::marker::PhantomData;

pub struct ExecThreadPool<I, O> {
    _phantom: PhantomData<(I, O)>,
}

impl<I, O> ExecThreadPool<I, O> {
    pub fn new(threads_count: usize) -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    pub fn add_data(packet: Packet<I>) {}

    pub fn set_output<X>(&mut self, target: &ExecThreadPool<O, X>) {}

    fn thread(&self) {
        // while let Some(work) = self.dispatch() {
        //     work.execute();
        // }
    }

    fn start() {}
}
