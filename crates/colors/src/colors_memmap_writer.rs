// use crate::storage::roaring::ColorsStorage;
use crate::storage::ColorsSerializerTrait;
use crate::storage::serializer::ColorsSerializer;
use config::ColorIndexType;
use dashmap::DashMap;
use hashes::dummy_hasher::DummyHasherBuilder;
use rand::{RngCore, thread_rng};
use siphasher::sip128::{Hasher128, SipHasher13};
use std::hash::Hash;
use std::mem::ManuallyDrop;
use std::path::Path;

pub struct ColorsMemMapWriter<C: ColorsSerializerTrait> {
    colors: DashMap<u128, ColorIndexType, DummyHasherBuilder>,
    colors_storage: ManuallyDrop<ColorsSerializer<C>>,
    hash_keys: (u64, u64),
}

impl<C: ColorsSerializerTrait> ColorsMemMapWriter<C> {
    pub fn new(
        file: impl AsRef<Path>,
        color_names: &[String],
        threads_count: usize,
        print_stats: bool,
    ) -> anyhow::Result<Self> {
        let mut rng = thread_rng();
        Ok(Self {
            colors: DashMap::with_hasher_and_shard_amount(
                DummyHasherBuilder,
                // Increase the number of shards to decrease stall while inserting new colors
                (rayon::current_num_threads() * 8).next_power_of_two(),
            ),
            colors_storage: ManuallyDrop::new(ColorsSerializer::new(
                file,
                color_names,
                threads_count,
                print_stats,
            )?),
            hash_keys: (rng.next_u64(), rng.next_u64()),
        })
    }

    fn hash_colors(&self, colors: &[ColorIndexType]) -> u128 {
        let mut hasher = SipHasher13::new_with_keys(self.hash_keys.0, self.hash_keys.1);
        colors.hash(&mut hasher);
        hasher.finish128().as_u128()
    }

    #[inline(always)]
    pub fn get_id(&self, colors: &[ColorIndexType]) -> ColorIndexType {
        let hash = self.hash_colors(colors);

        match self.colors.entry(hash) {
            dashmap::Entry::Occupied(occupied_entry) => *occupied_entry.get(),
            dashmap::Entry::Vacant(vacant_entry) => {
                let color = self.colors_storage.serialize_colors(colors);
                vacant_entry.insert(color);
                color
            }
        }
    }
}

impl<C: ColorsSerializerTrait> Drop for ColorsMemMapWriter<C> {
    fn drop(&mut self) {
        unsafe { ManuallyDrop::take(&mut self.colors_storage).finalize() };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DefaultColorsSerializer, colors_manager::ColorMapReader,
        storage::deserializer::ColorsDeserializer,
    };

    /// Equal sets must intern to one id, and the stored color map must give
    /// each id its colors back.
    #[test]
    fn interning_is_by_content_and_round_trips_through_the_color_map() {
        let path = std::env::temp_dir().join(format!("interner-{}.colors.dat", std::process::id()));
        let names: Vec<_> = (0..16).map(|i| format!("color_{i}")).collect();
        let writer =
            ColorsMemMapWriter::<DefaultColorsSerializer>::new(&path, &names, 1, false).unwrap();
        let id = writer.get_id(&[0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(writer.get_id(&[0, 1, 2, 3, 4, 5, 6, 7]), id);
        let other = writer.get_id(&[9, 10, 11, 12]);
        assert_ne!(id, other);
        drop(writer);

        let mut reader = ColorsDeserializer::<DefaultColorsSerializer>::new(&path, true).unwrap();
        assert_eq!(reader.colors_subsets_count(), 2);
        let mut colors = Vec::new();
        reader.get_color_mappings(id, &mut colors);
        assert_eq!(colors, [0, 1, 2, 3, 4, 5, 6, 7]);
        reader.get_color_mappings(other, &mut colors);
        assert_eq!(colors, [9, 10, 11, 12]);
        drop(reader);
        std::fs::remove_file(path).unwrap();
    }
}
