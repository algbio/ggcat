// use crate::storage::roaring::ColorsStorage;
use crate::storage::serializer::ColorsSerializer;
use crate::storage::ColorsSerializerTrait;
use config::ColorIndexType;
use dashmap::DashMap;
use hashes::dummy_hasher::DummyHasherBuilder;
use rand::{thread_rng, RngCore};
use siphasher::sip128::{Hasher128, SipHasher13};
use std::hash::Hash;
use std::path::Path;

pub struct ColorsMemMapWriter<C: ColorsSerializerTrait> {
    colors: DashMap<u128, ColorIndexType, DummyHasherBuilder>,
    colors_storage: ColorsSerializer<C>,
    hash_keys: (u64, u64),
}

impl<C: ColorsSerializerTrait> ColorsMemMapWriter<C> {
    pub fn new(file: impl AsRef<Path>, color_names: &[String]) -> anyhow::Result<Self> {
        let mut rng = thread_rng();
        Ok(Self {
            colors: DashMap::with_hasher_and_shard_amount(
                DummyHasherBuilder,
                // Increase the number of shards to decrease stall while inserting new colors
                (rayon::current_num_threads() * 8).next_power_of_two(),
            ),
            colors_storage: ColorsSerializer::new(file, color_names)?,
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

    pub fn print_stats(&self) {
        self.colors_storage.print_stats();
    }
}
