use crate::colors_manager::ColorMapReader;
use crate::storage::serializer::{ColorsFileHeader, ColorsIndexEntry, ColorsIndexMap};
use crate::storage::ColorsSerializerTrait;
use config::ColorIndexType;
use desse::Desse;
use desse::DesseSized;
use replace_with::replace_with_or_abort;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::Path;

pub struct ColorsDeserializer<DS: ColorsSerializerTrait> {
    colormap_file: lz4::Decoder<BufReader<File>>,
    #[allow(dead_code)]
    color_names: Vec<String>,
    colors_index: ColorsIndexMap,
    current_chunk: ColorsIndexEntry,
    current_chunk_size: ColorIndexType,
    current_index: ColorIndexType,
    _phantom: PhantomData<DS>,
}

unsafe impl<DS: ColorsSerializerTrait> Sync for ColorsDeserializer<DS> {}
unsafe impl<DS: ColorsSerializerTrait> Send for ColorsDeserializer<DS> {}

impl<DS: ColorsSerializerTrait> ColorsDeserializer<DS> {
    pub fn new(file: impl AsRef<Path>) -> Self {
        let mut file = File::open(file).unwrap();

        let mut header_buffer = [0; ColorsFileHeader::SIZE];
        file.read_exact(&mut header_buffer).unwrap();

        let header: ColorsFileHeader = ColorsFileHeader::deserialize_from(&header_buffer);
        assert_eq!(header.magic, DS::MAGIC);

        // println!("Colors header: {:#?}", &header);

        let color_names = {
            let mut compressed_stream = lz4::Decoder::new(BufReader::new(file)).unwrap();

            let color_names: Vec<String> =
                bincode::deserialize_from(&mut compressed_stream).unwrap();
            file = compressed_stream.finish().0.into_inner();
            color_names
        };

        let colors_index: ColorsIndexMap = {
            file.seek(SeekFrom::Start(header.index_offset)).unwrap();
            bincode::deserialize_from(&mut file).unwrap()
        };

        let first_chunk = colors_index.pairs[0];
        file.seek(SeekFrom::Start(first_chunk.file_offset)).unwrap();

        let current_chunk_size = colors_index
            .pairs
            .get(1)
            .map(|p| p.start_index)
            .unwrap_or(colors_index.subsets_count as ColorIndexType)
            - first_chunk.start_index;

        Self {
            colormap_file: lz4::Decoder::new(BufReader::new(file)).unwrap(),
            color_names,
            colors_index,
            current_chunk: first_chunk,
            current_chunk_size,
            current_index: first_chunk.start_index,
            _phantom: Default::default(),
        }
    }

    fn maybe_change_block(&mut self, target_color: ColorIndexType) {
        if target_color < self.current_index
            || target_color >= (self.current_chunk.start_index + self.current_chunk_size)
        {
            println!(
                "Changing chunk {} < {} || {} >= {} + {}!",
                target_color,
                self.current_index,
                target_color,
                self.current_chunk.start_index,
                self.current_chunk_size
            );
            // Requested color is outside of chunk range, update the current chunk
            let new_chunk_index = self
                .colors_index
                .pairs
                .partition_point(|x| x.start_index <= target_color)
                - 1;

            self.current_chunk = self.colors_index.pairs[new_chunk_index];
            self.current_chunk_size = self
                .colors_index
                .pairs
                .get(new_chunk_index + 1)
                .map(|p| p.start_index)
                .unwrap_or(self.colors_index.subsets_count as ColorIndexType)
                - self.current_chunk.start_index;
            self.current_index = self.current_chunk.start_index;

            replace_with_or_abort(&mut self.colormap_file, |colormap_file| {
                let mut buffered_file = colormap_file.finish().0;
                assert_ne!(self.current_chunk.file_offset, 0);
                buffered_file
                    .seek(SeekFrom::Start(self.current_chunk.file_offset))
                    .unwrap();
                lz4::Decoder::new(buffered_file).unwrap()
            });
        }
    }

    pub fn get_color_mappings(&mut self, color: ColorIndexType, out_vec: &mut Vec<ColorIndexType>) {
        self.maybe_change_block(color);

        while self.current_index < color {
            // Skip the colors
            DS::decode_color(&mut self.colormap_file, None);
            self.current_index += 1;
        }

        // Decode the requested color
        DS::decode_color(&mut self.colormap_file, Some(out_vec));
        self.current_index += 1;
    }
}

impl<DS: ColorsSerializerTrait> ColorMapReader for ColorsDeserializer<DS> {
    fn colors_count(&self) -> u64 {
        self.colors_index.subsets_count as u64
    }
}
