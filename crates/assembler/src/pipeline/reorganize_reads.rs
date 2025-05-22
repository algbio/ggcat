use assembler_kmers_merge::structs::PartialUnitigExtraData;
use config::{
    get_compression_level_info, get_memory_mode, BucketIndexType, SwapPriority,
    DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES,
};
use hashes::{HashFunctionFactory, HashableSequence};
use io::concurrent::temp_reads::creads_utils::{
    CompressedReadsBucketData, CompressedReadsBucketDataSerializer, NoMultiplicity, NoSecondBucket,
};
#[cfg(feature = "support_kmer_counters")]
use structs::unitigs_counters::UnitigsCounters;

use crate::structs::link_mapping::{LinkMapping, LinkMappingSerializer};
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use colors::colors_manager::{color_types, ColorsManager};
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use io::concurrent::structured_sequences::concurrent::FastaWriterConcurrentBuffer;
use io::concurrent::structured_sequences::{StructuredSequenceBackend, StructuredSequenceWriter};
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataConsecutiveCompression, SequenceExtraDataOwned,
    SequenceExtraDataTempBufferManagement,
};
use io::structs::unitig_link::UnitigIndex;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::{MultiThreadBuckets, SingleBucket};
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::io::{Read, Write};
use std::mem::transmute;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[cfg(feature = "support_kmer_counters")]
use io::concurrent::structured_sequences::SequenceAbundance;

#[derive(Clone, Debug)]
pub struct ReorganizedReadsExtraData<CX: SequenceExtraDataConsecutiveCompression> {
    pub unitig: UnitigIndex,
    pub colors: CX,
    #[cfg(feature = "support_kmer_counters")]
    pub counters: UnitigsCounters,
}

#[repr(transparent)]
pub struct ReorganizedReadsBuffer<CX: SequenceExtraDataConsecutiveCompression>(pub CX::TempBuffer);
impl<CX: SequenceExtraDataConsecutiveCompression> ReorganizedReadsBuffer<CX> {
    #[allow(dead_code)]
    pub fn from_inner_mut(inner: &mut CX::TempBuffer) -> &mut Self {
        unsafe { transmute(inner) }
    }
    pub fn from_inner(inner: &CX::TempBuffer) -> &Self {
        unsafe { transmute(inner) }
    }
}

impl<CX: SequenceExtraDataConsecutiveCompression> SequenceExtraDataTempBufferManagement
    for ReorganizedReadsExtraData<CX>
{
    type TempBuffer = ReorganizedReadsBuffer<CX>;

    #[inline(always)]
    fn new_temp_buffer() -> ReorganizedReadsBuffer<CX> {
        ReorganizedReadsBuffer(CX::new_temp_buffer())
    }

    #[inline(always)]
    fn clear_temp_buffer(buffer: &mut ReorganizedReadsBuffer<CX>) {
        CX::clear_temp_buffer(&mut buffer.0)
    }

    fn copy_temp_buffer(dest: &mut ReorganizedReadsBuffer<CX>, src: &ReorganizedReadsBuffer<CX>) {
        CX::copy_temp_buffer(&mut dest.0, &src.0)
    }

    #[inline(always)]
    fn copy_extra_from(
        extra: Self,
        src: &ReorganizedReadsBuffer<CX>,
        dst: &mut ReorganizedReadsBuffer<CX>,
    ) -> Self {
        let changed_color = CX::copy_extra_from(extra.colors, &src.0, &mut dst.0);
        Self {
            unitig: extra.unitig,
            colors: changed_color,
            #[cfg(feature = "support_kmer_counters")]
            counters: extra.counters,
        }
    }
}

impl<CX: SequenceExtraDataConsecutiveCompression> SequenceExtraDataConsecutiveCompression
    for ReorganizedReadsExtraData<CX>
{
    type LastData = CX::LastData;

    #[inline(always)]
    fn decode_extended(
        buffer: &mut Self::TempBuffer,
        mut reader: &mut impl Read,
        last_data: Self::LastData,
    ) -> Option<Self> {
        Some(Self {
            unitig: UnitigIndex::decode(&mut reader, ())?,
            colors: CX::decode_extended(&mut buffer.0, &mut reader, last_data)?,
            #[cfg(feature = "support_kmer_counters")]
            counters: <UnitigsCounters as SequenceExtraData>::decode_extended(&mut (), reader)?,
        })
    }

    #[inline(always)]
    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        mut writer: &mut impl Write,
        last_data: Self::LastData,
    ) {
        self.unitig.encode(&mut writer, ());
        self.colors
            .encode_extended(&buffer.0, &mut writer, last_data);
        #[cfg(feature = "support_kmer_counters")]
        <UnitigsCounters as SequenceExtraData>::encode_extended(
            &self.counters,
            &mut (),
            &mut writer,
        );
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        SequenceExtraData::max_size(&self.unitig)
            + self.colors.max_size()
            + match () {
                #[cfg(feature = "support_kmer_counters")]
                () => <UnitigsCounters as SequenceExtraData>::max_size(&self.counters),
                #[cfg(not(feature = "support_kmer_counters"))]
                () => 0,
            }
    }

    fn obtain_last_data(&self, last_data: Self::LastData) -> Self::LastData {
        self.colors.obtain_last_data(last_data)
    }
}

pub fn reorganize_reads<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    BK: StructuredSequenceBackend<PartialUnitigsColorStructure<CX>, ()>,
>(
    k: usize,
    mut reads: Vec<SingleBucket>,
    mut mapping_files: Vec<SingleBucket>,
    temp_path: &Path,
    out_file: &StructuredSequenceWriter<PartialUnitigsColorStructure<CX>, (), BK>,
    circular_out_file: Option<&StructuredSequenceWriter<PartialUnitigsColorStructure<CX>, (), BK>>,
    buckets_count: usize,
) -> (Vec<SingleBucket>, PathBuf) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: reads reorganization".to_string());

    let buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
        buckets_count,
        temp_path.join("reads_bucket"),
        None,
        &(
            get_memory_mode(SwapPriority::ReorganizeReads),
            CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            get_compression_level_info(),
        ),
        &(),
    ));

    reads.sort_by_key(|b| b.index);
    mapping_files.sort_by_key(|b| b.index);

    let inputs: Vec<_> = reads.iter().zip(mapping_files.iter()).collect();

    let reads_thread_buffers = ScopedThreadLocal::new(move || {
        BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, buckets_count)
    });

    inputs.par_iter().for_each(|(read_file, mapping_file)| {
        let mut buffers = reads_thread_buffers.get();

        let mut tmp_reads_buffer = BucketsThreadDispatcher::<
            _,
            CompressedReadsBucketDataSerializer<
                ReorganizedReadsExtraData<color_types::PartialUnitigsColorStructure<CX>>,
                typenum::U0,
                NoSecondBucket,
                NoMultiplicity,
            >,
        >::new(&buckets, buffers.take(), k);
        let mut tmp_lonely_unitigs_buffer =
            FastaWriterConcurrentBuffer::new(out_file, DEFAULT_OUTPUT_BUFFER_SIZE, true, k);

        let mut tmp_circular_unitigs_buffer = circular_out_file.map(|out_file| {
            FastaWriterConcurrentBuffer::new(out_file, DEFAULT_OUTPUT_BUFFER_SIZE, true, k)
        });

        let mut mappings = Vec::new();

        assert_eq!(read_file.index, mapping_file.index);

        let bucket_index = read_file.index as BucketIndexType;

        LockFreeBinaryReader::new(
            &mapping_file.path,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        )
        .decode_all_bucket_items::<LinkMappingSerializer, _>(
            (),
            &mut (),
            |link, _| {
                mappings.push(link);
            },
            (),
        );

        parallel_processor::make_comparer!(Compare, LinkMapping, entry: u64);
        fast_smart_radix_sort::<_, Compare, false>(&mut mappings[..]);

        let mut index = 0;
        let mut map_index = 0;

        let mut decompress_buffer = Vec::new();

        let mut colors_buffer = color_types::PartialUnitigsColorStructure::<CX>::new_temp_buffer();

        CompressedBinaryReader::new(
            &read_file.path,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        )
        .decode_all_bucket_items::<CompressedReadsBucketDataSerializer<
            PartialUnitigExtraData<color_types::PartialUnitigsColorStructure<CX>>,
            typenum::U0,
            NoSecondBucket,
            NoMultiplicity,
        >, _>(
            Vec::new(),
            &mut colors_buffer,
            |(_, _, extra_data, seq, _), color_buffer| {
                if seq.bases_count() > decompress_buffer.len() {
                    decompress_buffer.resize(seq.bases_count(), 0);
                }
                seq.write_unpacked_to_slice(&mut decompress_buffer[..seq.bases_count()]);

                let seq = &decompress_buffer[..seq.bases_count()];

                if map_index < mappings.len() && mappings[map_index].entry == index {
                    // Mapping found
                    tmp_reads_buffer.add_element_extended(
                        mappings[map_index].bucket,
                        &ReorganizedReadsExtraData {
                            unitig: UnitigIndex::new(bucket_index, index as usize, false),
                            colors: extra_data.colors,
                            #[cfg(feature = "support_kmer_counters")]
                            counters: extra_data.counters,
                        },
                        ReorganizedReadsBuffer::from_inner(color_buffer),
                        &CompressedReadsBucketData::new(seq, 0, 0),
                    );
                    map_index += 1;
                } else {
                    // Loop to allow skipping code parts with break
                    'skip_writing: loop {
                        let first_kmer_node = &seq[0..k - 1];
                        let last_kmer_node = &seq[seq.len() - k + 1..];
                        if let Some(circular_unitigs_buffer) = &mut tmp_circular_unitigs_buffer {
                            // Check if unitig is circular
                            if first_kmer_node == last_kmer_node {
                                circular_unitigs_buffer.add_read(
                                    seq,
                                    None,
                                    extra_data.colors,
                                    color_buffer,
                                    (),
                                    &(),
                                    #[cfg(feature = "support_kmer_counters")]
                                    SequenceAbundance {
                                        first: extra_data.counters.first,
                                        sum: extra_data.counters.sum,
                                        last: extra_data.counters.last,
                                    },
                                );
                                break 'skip_writing;
                            }
                        }

                        // No mapping, write unitig to file
                        tmp_lonely_unitigs_buffer.add_read(
                            seq,
                            None,
                            extra_data.colors,
                            color_buffer,
                            (),
                            &(),
                            #[cfg(feature = "support_kmer_counters")]
                            SequenceAbundance {
                                first: extra_data.counters.first,
                                sum: extra_data.counters.sum,
                                last: extra_data.counters.last,
                            },
                        );

                        break;
                    }
                    // write_fasta_entry::<MH, CX, _>(
                    //     &mut fasta_temp_buffer,
                    //     &mut tmp_lonely_unitigs_buffer,
                    //     color,
                    //     color_buffer,
                    //     seq,
                    //     0,
                    // );
                }

                color_types::PartialUnitigsColorStructure::<CX>::clear_temp_buffer(color_buffer);

                index += 1;
            },
            k,
        );

        buffers.put_back(tmp_reads_buffer.finalize().0);
        tmp_lonely_unitigs_buffer.finalize();

        assert_eq!(map_index, mappings.len())
    });

    (buckets.finalize_single(), PathBuf::new())
}
