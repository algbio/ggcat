use crate::storage::serializer::ColorsFlushProcessing;
use crate::storage::ColorsSerializerTrait;
use config::ColorIndexType;
use io::chunks_writer::ChunksWriter;
use parking_lot::Mutex;
use roaring::RoaringBitmap;
use std::io::Read;
use std::sync::atomic::{AtomicU32, Ordering};

struct RoaringBitmapInstance {
    bitmap: RoaringBitmap,
    offset: ColorIndexType,
    colors_count: u64,
    checkpoint_distance: u64,
    stride: ColorIndexType,
    last_color: ColorIndexType,
}

impl RoaringBitmapInstance {
    fn new(
        colors_count: u64,
        checkpoint_distance: u64,
        offset: ColorIndexType,
        stride: ColorIndexType,
    ) -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
            offset,
            colors_count,
            checkpoint_distance,
            stride,
            last_color: 0,
        }
    }

    fn try_append(
        &mut self,
        color_index: ColorIndexType,
        colors: impl Iterator<Item = ColorIndexType>,
        writer: &ColorsFlushProcessing,
    ) -> bool {
        let base_color = color_index - self.offset;

        // Another append is in queue and the current is not the first one
        if base_color > self.last_color + self.stride {
            return false;
        }

        self.last_color = base_color;

        assert_eq!(base_color % self.stride, 0);
        let strided_color = base_color / self.stride;

        let local_position = strided_color * (self.colors_count as u32);

        self.bitmap
            .append(colors.map(|c| local_position + c))
            .unwrap();

        // Flush the partial bitmap
        if strided_color >= self.checkpoint_distance as u32 {
            println!("Flushing with offset: {}", self.offset);
            self.flush(writer);
        }

        true
    }

    fn flush(&mut self, writer: &ColorsFlushProcessing) {
        let mut pdata = writer.start_processing();
        self.bitmap
            .serialize_into(writer.get_stream(&mut pdata))
            .unwrap();
        writer.end_processing(pdata, self.offset);
        self.offset += self.last_color;
        self.last_color = 0;
        self.bitmap.clear();
    }
}

pub struct RoaringColorsSerializer {
    colors_count: u64,
    roaring_bitmaps: Vec<Mutex<RoaringBitmapInstance>>,
    writer: ColorsFlushProcessing,
    colors_index: AtomicU32,
}

impl ColorsSerializerTrait for RoaringColorsSerializer {
    const MAGIC: [u8; 16] = *b"GGCAT_CMAP_ROARG";

    // FIXME: Implement!
    fn decode_color(_reader: impl Read, _out_vec: Option<&mut Vec<u32>>) {
        todo!()
    }

    fn new(writer: ColorsFlushProcessing, checkpoint_distance: usize, colors_count: u64) -> Self {
        let stride = rayon::current_num_threads() as ColorIndexType;

        Self {
            roaring_bitmaps: (0..stride)
                .map(|off| {
                    Mutex::new(RoaringBitmapInstance::new(
                        colors_count,
                        checkpoint_distance as u64,
                        off,
                        stride,
                    ))
                })
                .collect(),
            writer,
            colors_index: AtomicU32::new(0),
            colors_count,
        }
    }

    fn serialize_colors(&self, colors: &[ColorIndexType]) -> ColorIndexType {
        let color_index = self.colors_index.fetch_add(1, Ordering::Relaxed);

        let target_bitmap = color_index % self.roaring_bitmaps.len() as ColorIndexType;

        loop {
            let mut bitmap_lock = self.roaring_bitmaps[target_bitmap as usize].lock();
            if bitmap_lock.try_append(color_index, colors.iter().copied(), &self.writer) {
                break;
            }
            drop(bitmap_lock);
            std::thread::yield_now();
        }

        color_index
    }

    fn get_subsets_count(&self) -> u64 {
        self.colors_index.load(Ordering::Relaxed) as u64
    }

    fn print_stats(&self) {
        println!(
            "Subsets count: {} witn {} colors",
            self.get_subsets_count(),
            self.colors_count
        );
    }

    fn finalize(mut self) -> ColorsFlushProcessing {
        for bitmap in self.roaring_bitmaps {
            bitmap.lock().flush(&mut self.writer);
        }

        self.writer
    }
}
