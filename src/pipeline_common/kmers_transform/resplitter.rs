use crate::config::{
    SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL, DEFAULT_PER_CPU_BUFFER_SIZE,
    DEFAULT_PREFETCH_AMOUNT, MINIMIZER_BUCKETS_CHECKPOINT_SIZE, USE_SECOND_BUCKET,
};
use crate::hashes::HashableSequence;
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::pipeline_common::kmers_transform::reader::{InputBucketDesc, KmersTransformReader};
use crate::pipeline_common::kmers_transform::reads_buffer::ReadsBuffer;
use crate::pipeline_common::kmers_transform::{
    KmersTransformContext, KmersTransformExecutorFactory,
};
use crate::pipeline_common::minimizer_bucketing::{
    MinimizerBucketingExecutor, MinimizerBucketingExecutorFactory,
};
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::utils::get_memory_mode;
use crate::KEEP_FILES;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::async_binary_reader::AsyncBinaryReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::execution_manager::executor::{Executor, ExecutorType};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::Packet;
use parallel_processor::memory_fs::file::writer::FileWriter;
use parallel_processor::memory_fs::RemoveFileMode;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct KmersTransformResplitter<F: KmersTransformExecutorFactory> {
    context: Option<Arc<KmersTransformContext<F>>>,
    out_addresses: Option<Arc<Vec<ExecutorAddress>>>,
    resplitter:
        Option<<F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType>,
    thread_local_buffers: Option<BucketsThreadDispatcher<CompressedBinaryWriter>>,
    subsplit_buckets_count_log: usize,
}

impl<F: KmersTransformExecutorFactory> PoolObjectTrait for KmersTransformResplitter<F> {
    type InitData = ();

    fn allocate_new(_init_data: &Self::InitData) -> Self {
        Self {
            context: None,
            out_addresses: None,
            resplitter: None,
            thread_local_buffers: None,
            subsplit_buckets_count_log: 0,
        }
    }

    fn reset(&mut self) {
        self.context.take();
        self.resplitter.take();
    }
}

static BUCKET_RESPLIT_COUNTER: AtomicUsize = AtomicUsize::new(0);

impl<F: KmersTransformExecutorFactory> Executor for KmersTransformResplitter<F> {
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::SimplePacketsProcessing;

    const BASE_PRIORITY: u64 = 1;
    const PACKET_PRIORITY_MULTIPLIER: u64 = 1;
    const STRICT_POOL_ALLOC: bool = false;

    type InputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type OutputPacket = InputBucketDesc;
    type GlobalParams = KmersTransformContext<F>;
    type MemoryParams = ();
    type BuildParams = (
        Arc<KmersTransformContext<F>>,
        Arc<MultiThreadBuckets<CompressedBinaryWriter>>,
        usize,
        Arc<Vec<ExecutorAddress>>,
    );

    fn allocate_new_group<D: FnOnce(Vec<ExecutorAddress>)>(
        global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
        _common_packet: Option<Packet<Self::InputPacket>>,
        executors_initializer: D,
    ) -> (Self::BuildParams, usize) {
        let subsplit_buckets_count_log = 7; // FIXME!
        let buckets = Arc::new(MultiThreadBuckets::new(
            (1 << subsplit_buckets_count_log),
            global_params.temp_dir.join(format!(
                "resplit-bucket{}",
                BUCKET_RESPLIT_COUNTER.fetch_add(1, Ordering::Relaxed)
            )),
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
                DEFAULT_LZ4_COMPRESSION_LEVEL,
            ),
        ));

        let output_addresses: Vec<_> = (0..(1 << subsplit_buckets_count_log))
            .map(|_| KmersTransformReader::<F>::generate_new_address())
            .collect();
        global_params
            .extra_buckets_count
            .fetch_add((1 << subsplit_buckets_count_log), Ordering::Relaxed);
        executors_initializer(output_addresses.clone());

        // TODO: Find best count of writing threads
        let threads_count = global_params.read_threads_count;
        (
            (
                global_params,
                buckets,
                subsplit_buckets_count_log,
                Arc::new(output_addresses),
            ),
            threads_count,
        )
    }

    fn required_pool_items(&self) -> u64 {
        0
    }

    fn pre_execute<
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        reinit_params: Self::BuildParams,
        _packet_alloc: P,
        _packet_send: S,
    ) {
        self.resplitter = Some(F::new_resplitter(&reinit_params.0.global_extra_data));
        self.context = Some(reinit_params.0);
        self.out_addresses = Some(reinit_params.3);
        self.subsplit_buckets_count_log = reinit_params.2;
        self.thread_local_buffers = Some(BucketsThreadDispatcher::new(
            &reinit_params.1,
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, reinit_params.1.count()),
        ));
    }

    fn execute<
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        mut input_packet: Packet<Self::InputPacket>,
        _packet_alloc: P,
        _packet_send: S,
    ) {
        let input_packet = input_packet.deref_mut();

        let mut preproc_info = <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::PreprocessInfo::default();
        let resplitter = self.resplitter.as_mut().unwrap();
        let mut local_buffer = self.thread_local_buffers.as_mut().unwrap();

        for (flags, extra, bases) in input_packet.reads.drain(..) {
            let sequence = bases.as_reference(&input_packet.reads_buffer);

            resplitter.reprocess_sequence(flags, &extra, &mut preproc_info);
            resplitter.process_sequence::<_, _>(
                &preproc_info,
                sequence,
                0..sequence.bases_count(),
                |bucket, _sec_bucket, seq, flags, extra| {
                    local_buffer.add_element(
                        bucket % (1 << self.subsplit_buckets_count_log),
                        &extra,
                        &CompressedReadsBucketHelper::<
                            _,
                            <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::FLAGS_COUNT,
                            false,
                            true,
                        >::new_packed(seq, flags, 0),
                    );
                },
            );
        }
    }

    fn finalize<S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>)>(
        &mut self,
        mut packet_send: S,
    ) {
        if self.context.take().is_some() {
            self.resplitter.take();
            let buckets = self.thread_local_buffers.take().unwrap().finalize().1;
            if Arc::strong_count(&buckets) == 1 {
                for (i, bucket) in buckets.finalize().into_iter().enumerate() {
                    packet_send(
                        self.out_addresses.as_ref().unwrap()[i].clone(),
                        Packet::new_simple(InputBucketDesc {
                            path: bucket,
                            sub_bucket_counters: vec![],
                            resplitted: true,
                        }),
                    );
                }
            }
            self.out_addresses.take();
        }
    }

    fn is_finished(&self) -> bool {
        false
    }

    fn get_total_memory(&self) -> u64 {
        0
    }

    fn get_current_memory_params(&self) -> Self::MemoryParams {
        ()
    }
}
