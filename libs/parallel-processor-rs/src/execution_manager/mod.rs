pub mod async_channel;
pub mod execution_context;
pub mod executor;
pub mod executor_address;
pub mod manager;
pub mod memory_tracker;
pub mod objects_pool;
pub mod packet;
pub mod priority;
pub mod thread_pool;
pub mod units_io;
pub mod work_scheduler;

#[cfg(test)]
mod tests {
    use crate::execution_manager::execution_context::{ExecutionContext, PoolAllocMode};
    use crate::execution_manager::executor::{AsyncExecutor, ExecutorReceiver};
    use crate::execution_manager::objects_pool::PoolObjectTrait;
    use crate::execution_manager::packet::{Packet, PacketTrait};
    use crate::execution_manager::thread_pool::ExecThreadPool;
    use crate::execution_manager::units_io::{ExecutorInput, ExecutorInputAddressMode};
    use std::future::Future;
    use std::ops::Deref;
    use std::sync::Arc;
    use std::time::Duration;

    struct TestExecutor {}

    impl PoolObjectTrait for usize {
        type InitData = ();

        fn allocate_new(init_data: &Self::InitData) -> Self {
            0
        }

        fn reset(&mut self) {}
    }

    impl PacketTrait for usize {
        fn get_size(&self) -> usize {
            0
        }
    }

    impl AsyncExecutor for TestExecutor {
        type InputPacket = usize;
        type OutputPacket = usize;
        type GlobalParams = ();
        type AsyncExecutorFuture<'a> = impl Future<Output = ()> + 'a;

        fn new() -> Self {
            Self {}
        }

        fn async_executor_main<'a>(
            &'a mut self,
            global_params: &'a Self::GlobalParams,
            mut receiver: ExecutorReceiver<Self>,
        ) -> Self::AsyncExecutorFuture<'a> {
            async move {
                while let Ok(mut addr) = receiver.obtain_address().await {
                    while let Some(mut packet) = addr.receive_packet().await {
                        let mut x = *packet.deref();
                        for i in 0..100000000 {
                            x += i * x + i;
                        }
                        println!("X: {}", x);

                        tokio::time::sleep(Duration::from_millis(1000)).await;

                        drop(packet);
                        for exec in 0..2 {
                            let address = TestExecutor::generate_new_address();
                            addr.declare_addresses(vec![address.clone()]);

                            let mut packet = addr.packet_alloc().await;
                            *packet = exec + x;
                            println!("Push packet {}", *packet.deref() * 2 + exec);
                            addr.packet_send(address.clone(), packet);
                        }
                    }
                }
                println!("Ended executor!");
            }
        }
    }

    #[test]
    fn test_executors() {
        let context = ExecutionContext::new();

        let readers_pool = ExecThreadPool::new(&context, 16, "readers-pool");

        readers_pool.register_executors::<TestExecutor>(
            640000,
            PoolAllocMode::Shared { capacity: 1024 },
            (),
            &Arc::new(()),
        );

        let strings = vec![1]; //, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

        let mut test_input =
            ExecutorInput::from_iter(strings.into_iter(), ExecutorInputAddressMode::Multiple);

        test_input.set_output_executor::<TestExecutor>(&context);

        // readers_pool
        loop {
            std::thread::sleep(Duration::from_millis(1000));
        }
    }
}
