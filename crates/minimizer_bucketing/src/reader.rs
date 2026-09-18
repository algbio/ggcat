use crate::MinimzerBucketingFilesReaderInputPacket;
use crate::simd_batch::SimdSequencesBatch;
use crate::sink::BucketingSink;
use crate::{MinimizerBucketingExecutionContext, MinimizerBucketingExecutorFactory};
use io::sequences_stream::GenericSequencesStream;
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;

pub struct MinimizerBucketingFilesReader<
    Factory: MinimizerBucketingExecutorFactory,
    SequencesStream: GenericSequencesStream,
> {
    _phantom: PhantomData<(Factory::GlobalData, Factory::StreamInfo, SequencesStream)>,
}
unsafe impl<Factory: MinimizerBucketingExecutorFactory, SequencesStream: GenericSequencesStream>
    Sync for MinimizerBucketingFilesReader<Factory, SequencesStream>
{
}
unsafe impl<Factory: MinimizerBucketingExecutorFactory, SequencesStream: GenericSequencesStream>
    Send for MinimizerBucketingFilesReader<Factory, SequencesStream>
{
}

impl<Factory: MinimizerBucketingExecutorFactory, SequencesStream: GenericSequencesStream>
    MinimizerBucketingFilesReader<Factory, SequencesStream>
{
    fn execute(
        &self,
        context: &MinimizerBucketingExecutionContext<Factory>,
        ops: &ExecutorAddressOperations<Self>,
    ) {
        let mut sequences_stream = SequencesStream::new();
        let mut sink = BucketingSink::new(
            context.common.k,
            context.bases_per_lane,
            context.common.ignored_length,
            context.copy_ident,
            &context.packets_pool,
            &context.executor_group_address,
        );

        while let Some(input_packet) = ops.receive_packet() {
            context.current_file.fetch_add(1, Ordering::Relaxed);

            sink.begin_block(input_packet.stream_info.clone());
            let outcome = sequences_stream.read_block_into(&input_packet.sequences, &mut sink);
            if let Err(error) = outcome {
                ggcat_logging::error!("WARNING: Error while reading an input: {error:#}");
            }

            context.processed_files.fetch_add(1, Ordering::Relaxed);
        }

        sink.finish();
    }
}

impl<Factory: MinimizerBucketingExecutorFactory, SequencesStream: GenericSequencesStream>
    PoolObjectTrait for MinimzerBucketingFilesReaderInputPacket<Factory, SequencesStream>
{
    type InitData = ();

    fn allocate_new(_: &Self::InitData) -> Self {
        unreachable!()
    }

    fn reset(&mut self) {}
}

impl<
    Factory: MinimizerBucketingExecutorFactory,
    SequencesStream: GenericSequencesStream + Sync + Send + 'static,
> AsyncExecutor for MinimizerBucketingFilesReader<Factory, SequencesStream>
{
    type InputPacket = MinimzerBucketingFilesReaderInputPacket<Factory, SequencesStream>;
    type OutputPacket = SimdSequencesBatch<Factory::StreamInfo>;
    type GlobalParams = MinimizerBucketingExecutionContext<Factory>;
    type InitData = ();
    const ALLOW_PARALLEL_ADDRESS_EXECUTION: bool = true;

    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    fn executor_main<'a>(
        &'a mut self,
        global_params: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
    ) {
        while let Ok(address) = receiver.obtain_address() {
            self.execute(global_params, &address);
        }
    }
}
