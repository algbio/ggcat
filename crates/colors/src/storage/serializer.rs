use crate::storage::ColorsSerializerTrait;
use config::{COLORS_SINGLE_BATCH_SIZE, ColorIndexType};
use crossbeam::channel::Sender;
use desse::{Desse, DesseSized};
use ggcat_logging::UnrecoverableErrorLogging;
use parallel_processor::execution_manager::objects_pool::{ObjectsPool, PoolObject};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;

pub const COLORMAP_STORAGE_VERSION: u64 = 1;

#[derive(Debug, Desse, DesseSized, Default)]
pub(crate) struct ColorsFileHeader {
    pub magic: [u8; 16],
    pub version: u64,
    pub index_offset: u64,
    pub colors_count: u64,
    pub subsets_count: u64,
    pub total_size: u64,
    pub total_uncompressed_size: u64,
}

#[derive(Clone, Copy, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct ColorsIndexEntry {
    pub start_index: ColorIndexType,
    pub file_offset: u64,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ColorsIndexMap {
    pub pairs: Vec<ColorsIndexEntry>,
    pub subsets_count: u64,
}

pub struct ColorsSerializer<SI: ColorsSerializerTrait> {
    colors_subset_count: Mutex<ColorIndexType>,
    buffers_pool: ObjectsPool<SI::PreSerializer>,
    colors_sender: Sender<PoolObject<SI::PreSerializer>>,

    checkpoint_tracker: Arc<Mutex<SI::CheckpointTracker>>,
    serializer: Arc<SI>,

    writing_threads: Vec<JoinHandle<()>>,
    print_stats: bool,
    _phantom: PhantomData<SI>,
}

impl<SI: ColorsSerializerTrait> ColorsSerializer<SI> {
    pub fn new(
        file: impl AsRef<Path>,
        color_names: &[String],
        threads_count: usize,
        print_stats: bool,
    ) -> anyhow::Result<Self> {
        let mut colormap_file = File::create(file.as_ref()).log_unrecoverable_error_with_data(
            "Cannot create colormap file",
            file.as_ref().display(),
        )?;

        colormap_file
            .write_all(&ColorsFileHeader::default().serialize()[..])
            .log_unrecoverable_error_with_data(
                "Cannot write colormap header",
                file.as_ref().display(),
            )?;

        colormap_file = {
            let mut color_names_stream = lz4::EncoderBuilder::new()
                .level(4)
                .build(colormap_file)
                .unwrap();
            bincode::serialize_into(&mut color_names_stream, color_names)
                .log_unrecoverable_error_with_data(
                    "Cannot serialize color names",
                    file.as_ref().display(),
                )?;

            let (cf, res) = color_names_stream.finish();
            res.log_unrecoverable_error_with_data(
                "Cannot finish color names stream",
                file.as_ref().display(),
            )?;
            cf
        };

        let file_offset = colormap_file
            .stream_position()
            .log_unrecoverable_error_with_data(
                "Cannot seek colormap file",
                file.as_ref().display(),
            )?;

        let color_processor = ColorsFlushProcessing {
            colormap_file: BufWriter::new(colormap_file),
            colormap_index: ColorsIndexMap {
                pairs: vec![],
                subsets_count: 0,
            },
            offset: file_offset,
            uncompressed_size: 0,
        };

        let colors_count = color_names.len() as u64;

        let (colors_sender, receiver) =
            crossbeam::channel::bounded::<PoolObject<SI::PreSerializer>>(128);
        let buffers_pool = ObjectsPool::new(128, ());

        let (serializer, checkpoint_tracker) = SI::new(
            color_processor,
            COLORS_SINGLE_BATCH_SIZE as usize,
            colors_count,
        );

        let checkpoint_tracker = Arc::new(Mutex::new(checkpoint_tracker));
        let serializer = Arc::new(serializer);

        let mut writing_threads = Vec::with_capacity(threads_count);
        for thread in 0..threads_count {
            let checkpoint_tracker = checkpoint_tracker.clone();
            let serializer = serializer.clone();
            let receiver = receiver.clone();

            writing_threads.push(
                std::thread::Builder::new()
                    .name(format!("cmap-write-{}", thread))
                    .spawn(move || {
                        let mut checkpoint_tracker_guard = checkpoint_tracker.lock();
                        let mut checkpoint_buffer = SI::CheckpointBuffer::default();
                        let mut compressed_checkpoint_buffer =
                            SI::CompressedCheckpointBuffer::default();

                        while let Ok(colors) = receiver.recv() {
                            if let Some(flush_checkpoint) = SI::write_color_subset(
                                &mut checkpoint_tracker_guard,
                                &mut checkpoint_buffer,
                                &colors,
                            ) {
                                drop(checkpoint_tracker_guard);
                                serializer.flush_checkpoint(
                                    flush_checkpoint,
                                    &mut compressed_checkpoint_buffer,
                                );
                                checkpoint_tracker_guard = checkpoint_tracker.lock();
                            }
                        }

                        serializer.final_flush_buffer(
                            &mut checkpoint_tracker_guard,
                            checkpoint_buffer,
                            compressed_checkpoint_buffer,
                        );
                    })
                    .unwrap(),
            );
        }

        Ok(Self {
            colors_subset_count: Mutex::new(0),
            buffers_pool,
            colors_sender,
            checkpoint_tracker,
            serializer,
            writing_threads,
            print_stats,
            _phantom: PhantomData,
        })
    }

    #[inline(always)]
    pub fn serialize_colors(&self, colors: &[ColorIndexType]) -> ColorIndexType {
        let mut colors_buffer = self.buffers_pool.alloc_object();
        SI::preserialize_colors(&mut colors_buffer, colors);
        let mut colors_subset_count = self.colors_subset_count.lock();
        let new_color = *colors_subset_count;
        *colors_subset_count += 1;
        self.colors_sender.send(colors_buffer).unwrap();
        new_color
    }

    pub fn finalize(self) {
        // Drop the sender to free the writing threads
        drop(self.colors_sender);

        for thread in self.writing_threads {
            thread.join().unwrap();
        }

        let tracker = Arc::try_unwrap(self.checkpoint_tracker)
            .unwrap_or_else(|_| unreachable!())
            .into_inner();
        let serializer = Arc::try_unwrap(self.serializer).unwrap_or_else(|_| unreachable!());

        if self.print_stats {
            SI::print_stats(&tracker);
        }

        serializer.finalize(tracker);
    }
}

pub struct ColorsFlushProcessing {
    pub(crate) colormap_file: BufWriter<File>,
    pub(crate) colormap_index: ColorsIndexMap,
    pub(crate) offset: u64,
    pub(crate) uncompressed_size: u64,
}

impl ColorsFlushProcessing {
    pub fn compress_chunk(data: &[u8], out_data: &mut Vec<u8>) {
        let mut encoder = lz4::EncoderBuilder::new().level(4).build(out_data).unwrap();
        encoder.write_all(data).unwrap();

        let (_, res) = encoder.finish();
        res.unwrap();
    }

    pub fn write_compressed_chunk(
        &mut self,
        start_index: ColorIndexType,
        uncompressed_size: usize,
        compressed_data: &[u8],
    ) {
        self.uncompressed_size += uncompressed_size as u64;
        let file_offset = self.offset;
        self.colormap_file.write_all(compressed_data).unwrap();
        self.offset += compressed_data.len() as u64;

        self.colormap_index.pairs.push(ColorsIndexEntry {
            start_index,
            file_offset,
        });
    }
}
