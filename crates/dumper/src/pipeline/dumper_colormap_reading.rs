use crate::pipeline::dumper_minimizer_bucketing::DumperKmersReferenceData;
use colors::colors_manager::color_types::SingleKmerColorDataType;
use colors::colors_manager::ColorsManager;
use colors::storage::deserializer::ColorsDeserializer;
use colors::storage::ColorsSerializerTrait;
use config::{ColorIndexType, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES};
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::temp_reads::creads_utils::{
    CompressedReadsBucketDataSerializer, NoMultiplicity, NoSecondBucket,
};
use nightly_quirks::slice_group_by::SliceGroupBy;
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::buckets::SingleBucket;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, FastSortable, SortKey};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use parking_lot::Mutex;
use rayon::prelude::*;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

pub fn colormap_reading<
    CX: ColorsManager<SingleKmerColorDataType = ColorIndexType>,
    CD: ColorsSerializerTrait,
>(
    k: usize,
    colormap_file: PathBuf,
    colored_unitigs_buckets: Vec<SingleBucket>,
    single_thread_output_function: bool,
    output_function: impl Fn(&[u8], &[ColorIndexType], bool) + Send + Sync,
) -> anyhow::Result<()> {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: colormap reading".to_string());

    // Try to build a color deserializer to check colormap correctness
    let _ = ColorsDeserializer::<CD>::new(&colormap_file, false)?;

    let tlocal_colormap_decoder = ScopedThreadLocal::new(move || {
        ColorsDeserializer::<CD>::new(&colormap_file, false).unwrap()
    });

    let single_thread_lock = Mutex::new(());

    colored_unitigs_buckets.par_iter().for_each(|input| {
        let mut colormap_decoder = tlocal_colormap_decoder.get();
        let mut temp_colors_buffer = Vec::new();
        let mut temp_decompressed_sequence = Vec::new();

        let mut temp_bases = Vec::new();
        let mut temp_sequences = Vec::new();

        CompressedBinaryReader::new(
            &input.path,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        )
        .decode_all_bucket_items::<CompressedReadsBucketDataSerializer<
            DumperKmersReferenceData<SingleKmerColorDataType<CX>>,
            typenum::consts::U0,
            NoSecondBucket,
            NoMultiplicity,
        >, _>(
            vec![],
            &mut (),
            |(_, _, color_extra, read, _), _| {
                let new_read = CompressedReadIndipendent::from_read(&read, &mut temp_bases);
                temp_sequences.push((new_read, color_extra));
            },
            k,
        );

        struct ColoredUnitigsCompare<CX: ColorsManager>(PhantomData<&'static CX>);
        impl<CX: ColorsManager>
            SortKey<(
                CompressedReadIndipendent,
                DumperKmersReferenceData<SingleKmerColorDataType<CX>>,
            )> for ColoredUnitigsCompare<CX>
        {
            type KeyType = SingleKmerColorDataType<CX>;
            const KEY_BITS: usize = std::mem::size_of::<SingleKmerColorDataType<CX>>() * 8;

            fn compare(
                left: &(
                    CompressedReadIndipendent,
                    DumperKmersReferenceData<SingleKmerColorDataType<CX>>,
                ),
                right: &(
                    CompressedReadIndipendent,
                    DumperKmersReferenceData<SingleKmerColorDataType<CX>>,
                ),
            ) -> std::cmp::Ordering {
                left.1.cmp(&right.1)
            }

            fn get_shifted(
                value: &(
                    CompressedReadIndipendent,
                    DumperKmersReferenceData<SingleKmerColorDataType<CX>>,
                ),
                rhs: u8,
            ) -> u8 {
                value.1.color.get_shifted(rhs)
            }
        }

        fast_smart_radix_sort::<_, ColoredUnitigsCompare<CX>, false>(&mut temp_sequences[..]);

        for unitigs_by_color in temp_sequences.nq_group_by_mut(|a, b| a.1 == b.1) {
            let color = unitigs_by_color[0].1.color;
            temp_colors_buffer.clear();
            colormap_decoder.get_color_mappings(color, &mut temp_colors_buffer);

            let mut same_color = false;

            let _lock = if single_thread_output_function {
                Some(single_thread_lock.lock())
            } else {
                None
            };

            for unitig in unitigs_by_color {
                let read = unitig.0.as_reference(&temp_bases);
                temp_decompressed_sequence.clear();
                temp_decompressed_sequence.extend(read.as_bases_iter());
                output_function(&temp_decompressed_sequence, &temp_colors_buffer, same_color);
                same_color = true;
            }
        }
    });
    Ok(())
}
