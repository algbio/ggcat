use crate::config::{DEFAULT_PREFETCH_AMOUNT, USE_SECOND_BUCKET};
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::pipeline_common::kmers_transform::processor::KmersTransformProcessor;
use crate::pipeline_common::kmers_transform::reads_buffer::ReadsBuffer;
use crate::pipeline_common::kmers_transform::{
    KmersTransformContext, KmersTransformExecutorFactory, KmersTransformPreprocessor,
};
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::KEEP_FILES;
use itertools::Itertools;
use parallel_processor::buckets::readers::async_binary_reader::AsyncBinaryReader;
use parallel_processor::execution_manager::executor::{Executor, ExecutorType};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::memory_fs::RemoveFileMode;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct KmersTransformReader<F: KmersTransformExecutorFactory> {
    context: Option<Arc<KmersTransformContext<F>>>,
    second_buckets_count_log: usize,
    buffers: Vec<Packet<ReadsBuffer<F::AssociatedExtraData>>>,
    addresses: Vec<ExecutorAddress>,
    preprocessor: Option<F::PreprocessorType>,
    async_reader: Option<AsyncBinaryReader>,
    _phantom: PhantomData<F>,
}

pub struct InputBucketDesc {
    pub(crate) path: PathBuf,
    pub(crate) resplitted: bool,
}

impl PoolObjectTrait for InputBucketDesc {
    type InitData = ();

    fn allocate_new(init_data: &Self::InitData) -> Self {
        Self {
            path: PathBuf::new(),
            resplitted: false,
        }
    }

    fn reset(&mut self) {
        self.resplitted = false;
    }
}
impl PacketTrait for InputBucketDesc {}

impl<F: KmersTransformExecutorFactory> PoolObjectTrait for KmersTransformReader<F> {
    type InitData = ();

    fn allocate_new(_init_data: &Self::InitData) -> Self {
        Self {
            context: None,
            second_buckets_count_log: 0,
            buffers: vec![],
            addresses: vec![],
            preprocessor: None,
            async_reader: None,
            _phantom: Default::default(),
        }
    }

    fn reset(&mut self) {
        self.context.take();
        self.buffers.clear();
        self.addresses.clear();
        self.preprocessor.take();
        self.async_reader.take();
    }
}

impl<F: KmersTransformExecutorFactory> Executor for KmersTransformReader<F> {
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::MultipleCommonPacketUnits;

    type InputPacket = InputBucketDesc;
    type OutputPacket = ReadsBuffer<F::AssociatedExtraData>;
    type GlobalParams = KmersTransformContext<F>;
    type MemoryParams = ();
    type BuildParams = (Arc<KmersTransformContext<F>>, AsyncBinaryReader, usize);

    fn allocate_new_group(
        global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
        common_packet: Option<Packet<Self::InputPacket>>,
    ) -> Self::BuildParams {
        let file = common_packet.unwrap();

        (
            global_params,
            AsyncBinaryReader::new(
                &file.path,
                true,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
                DEFAULT_PREFETCH_AMOUNT,
            ),
            4, // FIXME: Choose the right number of executors depending on size
        )
    }

    fn get_maximum_concurrency(&self) -> usize {
        4 // TODO: Optimize for bucket size
    }

    fn reinitialize<P: FnMut() -> Packet<Self::OutputPacket>>(
        &mut self,
        reinit_params: &Self::BuildParams,
        mut packet_alloc: P,
    ) {
        self.context = Some(reinit_params.0.clone());

        let second_buckets_count_log = reinit_params.2;
        let second_buckets_count = (1 << second_buckets_count_log);

        self.buffers
            .extend((0..second_buckets_count).map(|_| packet_alloc()));

        // TODO: Enable resplitting
        self.addresses.extend(
            (0..second_buckets_count).map(|_| KmersTransformProcessor::<F>::generate_new_address()),
        );

        self.preprocessor = Some(F::new_preprocessor(
            &self.context.as_ref().unwrap().global_extra_data,
        ));

        self.async_reader = Some(reinit_params.1.clone());
    }

    fn pre_execute<
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        mut packet_alloc: P,
        mut packet_send: S,
    ) {
        let async_reader_thread = self.context.as_ref().unwrap().async_readers.get();
        let preprocessor = self.preprocessor.as_ref().unwrap();
        let global_extra_data = &self.context.as_ref().unwrap().global_extra_data;

        self.async_reader
            .as_ref()
            .unwrap()
            .decode_all_bucket_items::<CompressedReadsBucketHelper<
                F::AssociatedExtraData,
                F::FLAGS_COUNT,
                { USE_SECOND_BUCKET },
                false,
            >, _>(async_reader_thread.clone(), Vec::new(), |read_info| {
                let bucket = preprocessor.get_sequence_bucket(global_extra_data, &read_info)
                    as usize
                    % (1 << self.second_buckets_count_log);

                let (flags, _second_bucket, extra_data, read) = read_info;

                let ind_read = CompressedReadIndipendent::from_read(
                    &read,
                    &mut self.buffers[bucket].reads_buffer,
                );
                self.buffers[bucket]
                    .reads
                    .push((flags, extra_data, ind_read));
                if self.buffers[bucket].reads.len() == self.buffers[bucket].reads.capacity() {
                    println!("Sending packet {}!", bucket);
                    packet_send(
                        self.addresses[bucket].clone(),
                        std::mem::replace(&mut self.buffers[bucket], packet_alloc()),
                    );
                }
            });
        for (packet, address) in self.buffers.drain(..).zip(self.addresses.drain(..)) {
            if packet.reads.len() > 0 {
                packet_send(address, packet);
            }
        }
    }

    fn execute<
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        _input_packet: Packet<Self::InputPacket>,
        _packet_alloc: P,
        _packet_send: S,
    ) {
        panic!("Multiple packet processing not supported!");
    }

    fn finalize<S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>)>(&mut self, _packet_send: S) {
    }

    fn get_total_memory(&self) -> u64 {
        0
    }

    fn get_current_memory_params(&self) -> Self::MemoryParams {
        ()
    }
}
