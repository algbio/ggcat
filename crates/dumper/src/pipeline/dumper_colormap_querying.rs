use colors::colors_manager::ColorsManager;
use colors::storage::deserializer::ColorsDeserializer;
use colors::storage::ColorsSerializerTrait;
use config::ColorIndexType;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use parking_lot::Mutex;
use rayon::prelude::*;
use std::path::PathBuf;

pub fn colormap_query<
    CX: ColorsManager<SingleKmerColorDataType = ColorIndexType>,
    CD: ColorsSerializerTrait,
>(
    colormap_file: PathBuf,
    mut color_subsets: Vec<ColorIndexType>,
    single_thread_output_function: bool,
    output_function: impl Fn(ColorIndexType, &[ColorIndexType]) + Send + Sync,
) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: colormap query".to_string());

    let tlocal_colormap_decoder =
        ScopedThreadLocal::new(move || ColorsDeserializer::<CD>::new(&colormap_file, false));

    let single_thread_lock = Mutex::new(());

    color_subsets.sort_unstable();
    color_subsets.dedup();

    color_subsets.par_iter().chunks(100).for_each(|subsets| {
        let mut colormap_decoder = tlocal_colormap_decoder.get();
        let mut temp_colors_buffer = Vec::new();

        for &color in subsets {
            temp_colors_buffer.clear();
            colormap_decoder.get_color_mappings(color, &mut temp_colors_buffer);

            let _lock = if single_thread_output_function {
                Some(single_thread_lock.lock())
            } else {
                None
            };

            output_function(color, &temp_colors_buffer[..]);
        }
    });
}
