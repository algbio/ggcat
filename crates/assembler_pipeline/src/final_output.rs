use std::{any::Any, path::Path, sync::Arc};

use ::dynamic_dispatch::dynamic_dispatch;
use colors::colors_manager::ColorsManager;
use sequence_output::structured_sequences::{
    StructuredSequenceBackendWrapper, StructuredSequenceWriter,
    concurrent::DynSequenceWriter,
    fasta::FastaWriterWrapper,
    gfa::{GFAWriterWrapperV1, GFAWriterWrapperV2},
};

use crate::get_final_output_writer;

/// The writer of the final output in the format `OutputMode`, boxed as an
/// `Arc<dyn DynSequenceWriter<CX>>`: the code writing to it is compiled once for all the formats.
#[dynamic_dispatch(CX = [
    #[cfg(feature = "enable-colors")] colors::bundles::multifile_building::ColorBundleMultifileBuilding,
    colors::non_colored::NonColoredManager,
], OutputMode = [
    FastaWriterWrapper,
    #[cfg(feature = "enable-gfa")] GFAWriterWrapperV1,
    #[cfg(feature = "enable-gfa")] GFAWriterWrapperV2
])]
pub fn new_final_output_writer<CX: ColorsManager, OutputMode: StructuredSequenceBackendWrapper>(
    output_file: &Path,
    k: usize,
) -> Box<dyn Any + Send> {
    let writer: Arc<dyn DynSequenceWriter<CX>> = Arc::new(StructuredSequenceWriter::new(
        get_final_output_writer::<_, (), OutputMode::Backend<_, _>>(output_file),
        k,
    ));
    Box::new(writer)
}
