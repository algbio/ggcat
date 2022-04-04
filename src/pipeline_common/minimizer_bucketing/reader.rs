use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::sequences_reader::SequencesReader;
use crate::pipeline_common::minimizer_bucketing::queue_data::MinimizerBucketingQueueData;
use crate::pipeline_common::minimizer_bucketing::MinimizerBucketingExecutionContext;
use nightly_quirks::branch_pred::unlikely;
use parallel_processor::threadpools_chain::ObjectsPoolManager;
use std::mem::swap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

pub fn minb_reader<
    ReadAssociatedData: SequenceExtraData,
    GlobalData,
    FileInfo: Clone + Sync + Send + Default,
>(
    context: &MinimizerBucketingExecutionContext<ReadAssociatedData, GlobalData>,
    manager: ObjectsPoolManager<MinimizerBucketingQueueData<FileInfo>, (PathBuf, FileInfo)>,
) {
    while let Some((input, file_info)) = manager.recv_obj() {
        let mut data = manager.allocate();
        data.file_info = file_info.clone();
        data.start_read_index = 0;

        let mut read_index = 0;

        context.current_file.fetch_add(1, Ordering::Relaxed);

        #[cfg(not(feature = "minimizer-read-disable"))]
        SequencesReader::process_file_extended(
            input,
            |x| {
                if x.seq.len() < context.common.k {
                    return;
                }

                if unlikely(!data.push_sequences(x)) {
                    let mut tmp_data = manager.allocate();

                    swap(&mut data, &mut tmp_data);
                    data.file_info = file_info.clone();
                    data.start_read_index = read_index;

                    assert!(
                        tmp_data.start_read_index as usize + tmp_data.sequences.len()
                            <= read_index as usize
                    );

                    #[cfg(not(feature = "minimizer-read-queue-disable"))]
                    manager.send(tmp_data);

                    if !data.push_sequences(x) {
                        panic!("Out of memory!");
                    }
                }
                read_index += 1;
            },
            false,
        );
        if data.sequences.len() > 0 {
            manager.send(data);
        } else {
            manager.deallocate(data);
        }
    }
}
