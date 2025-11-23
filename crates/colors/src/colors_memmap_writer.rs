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
