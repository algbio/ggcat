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
    pub fn new(file: impl AsRef<Path>, color_names: &[String]) -> Self {
        let mut rng = thread_rng();
        Self {
            colors: DashMap::with_hasher(DummyHasherBuilder),
            colors_storage: ColorsSerializer::new(file, color_names),
            hash_keys: (rng.next_u64(), rng.next_u64()),
        }
    }

    fn hash_colors(&self, colors: &[ColorIndexType]) -> u128 {
        let mut hasher = SipHasher13::new_with_keys(self.hash_keys.0, self.hash_keys.1);
        colors.hash(&mut hasher);
        hasher.finish128().as_u128()
    }

    pub fn get_id(&self, colors: &[ColorIndexType]) -> ColorIndexType {
        let hash = self.hash_colors(colors);

        match self.colors.get(&hash) {
            None => {
                let color = self.colors_storage.serialize_colors(colors);
                self.colors.insert(hash, color);
                color
            }
            Some(id) => *id,
        }
    }

    pub fn print_stats(&self) {
        self.colors_storage.print_stats();
    }
}
