// use crate::storage::roaring::ColorsStorage;
use crate::bucket_colors::ColorRun;
use crate::color_assigner::ColorRemap;
use crate::storage::ColorsSerializerTrait;
use crate::storage::serializer::ColorsSerializer;
use config::ColorIndexType;
use dashmap::DashMap;
use hashes::dummy_hasher::DummyHasherBuilder;
use rand::{RngCore, thread_rng};
use siphasher::sip128::{Hasher128, SipHasher13};
use std::cell::RefCell;
use std::hash::Hash;
use std::mem::ManuallyDrop;
use std::path::Path;

pub struct ColorsMemMapWriter<C: ColorsSerializerTrait> {
    colors: DashMap<u128, ColorIndexType, DummyHasherBuilder>,
    colors_storage: ManuallyDrop<ColorsSerializer<C>>,
    hash_keys: (u64, u64),
    /// Where the colors of the sets go in the colormap, if not where they are.
    remap: Option<ColorRemap>,
}

thread_local! {
    /// The runs of a set remapped, as it is written.
    static REMAPPED_RUNS: RefCell<Vec<ColorRun>> = const { RefCell::new(Vec::new()) };
}

impl<C: ColorsSerializerTrait> ColorsMemMapWriter<C> {
    /// The colormap of the colors `color_names` name, in `file`. With a `remap`, the sets given
    /// to [`Self::get_id`] are of the colors before it, and are written remapped: `color_names`
    /// names the colors after it.
    pub fn new(
        file: impl AsRef<Path>,
        color_names: &[String],
        remap: Option<ColorRemap>,
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
            remap,
        })
    }

    /// Hashes the canonical run list rather than the colors it stands for.
    ///
    /// The keys are drawn per process and never written anywhere, so what is fed
    /// to the hash is free to change; what is not free to change is that one set
    /// has exactly one spelling, or the same set would get two ids. Coalescing
    /// in [`crate::bucket_colors::canonicalize`] is what guarantees it, and every
    /// caller goes through it before looking up an id.
    fn hash_colors(&self, colors: &[ColorRun]) -> u128 {
        let mut hasher = SipHasher13::new_with_keys(self.hash_keys.0, self.hash_keys.1);
        colors.hash(&mut hasher);
        hasher.finish128().as_u128()
    }

    /// The id of the set of `colors`, in canonical form, written to the colormap the first time.
    /// Sets are told apart before any remap, as a remap moves the colors of different sets to
    /// different ones: a set is remapped only when it is written.
    #[inline(always)]
    pub fn get_id(&self, colors: &[ColorRun]) -> ColorIndexType {
        let hash = self.hash_colors(colors);

        match self.colors.entry(hash) {
            dashmap::Entry::Occupied(occupied_entry) => *occupied_entry.get(),
            dashmap::Entry::Vacant(vacant_entry) => {
                let color = match &self.remap {
                    Some(remap) => REMAPPED_RUNS.with_borrow_mut(|runs| {
                        self.colors_storage
                            .serialize_colors(remap.map_runs(colors, runs))
                    }),
                    None => self.colors_storage.serialize_colors(colors),
                };
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

    /// Equal sets must get the same id, and the stored color map must give
    /// each id its colors back.
    #[test]
    fn equal_sets_get_one_id_and_round_trip_through_the_color_map() {
        let path =
            std::env::temp_dir().join(format!("color-ids-{}.colors.dat", std::process::id()));
        let names: Vec<_> = (0..16).map(|i| format!("color_{i}")).collect();
        let writer =
            ColorsMemMapWriter::<DefaultColorsSerializer>::new(&path, &names, None, 1, false)
                .unwrap();
        let id = writer.get_id(&[ColorRun::new(0, 8)]);
        assert_eq!(writer.get_id(&[ColorRun::new(0, 8)]), id);
        let other = writer.get_id(&[ColorRun::new(9, 4)]);
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

    /// With a remap, sets are told apart by their colors before it, and written after it.
    #[test]
    fn sets_are_written_remapped() {
        let path = std::env::temp_dir().join(format!(
            "color-ids-remapped-{}.colors.dat",
            std::process::id()
        ));
        let names: Vec<_> = (0..12).map(|i| format!("color_{i}")).collect();
        // Blocks of four: the first stays, the second goes up four, the third down four.
        let remap = ColorRemap::from_parts(2, vec![0, 4, 4u32.wrapping_neg()]);
        let writer = ColorsMemMapWriter::<DefaultColorsSerializer>::new(
            &path,
            &names,
            Some(remap),
            1,
            false,
        )
        .unwrap();
        // 5, 6, 7 and 8 are consecutive before the remap, not after: 9, 10, 11 and 4.
        let set = [ColorRun::new(1, 1), ColorRun::new(5, 4)];
        let id = writer.get_id(&set);
        assert_eq!(writer.get_id(&set), id);
        let other = writer.get_id(&[ColorRun::new(8, 1)]);
        assert_ne!(id, other);
        drop(writer);

        let mut reader = ColorsDeserializer::<DefaultColorsSerializer>::new(&path, true).unwrap();
        assert_eq!(reader.colors_subsets_count(), 2);
        let mut colors = Vec::new();
        reader.get_color_mappings(id, &mut colors);
        assert_eq!(colors, [1, 4, 9, 10, 11]);
        reader.get_color_mappings(other, &mut colors);
        assert_eq!(colors, [4]);
        drop(reader);
        std::fs::remove_file(path).unwrap();
    }

    /// A color map without a single set, as an input with no sequences leaves, can be opened,
    /// and asking it for a set is a bounds error rather than a decode of whatever follows.
    #[test]
    fn a_color_map_without_sets_opens() {
        let path =
            std::env::temp_dir().join(format!("color-ids-empty-{}.colors.dat", std::process::id()));
        let names: Vec<_> = (0..4).map(|i| format!("color_{i}")).collect();
        drop(
            ColorsMemMapWriter::<DefaultColorsSerializer>::new(&path, &names, None, 1, false)
                .unwrap(),
        );

        let mut reader = ColorsDeserializer::<DefaultColorsSerializer>::new(&path, true).unwrap();
        assert_eq!(reader.colors_subsets_count(), 0);
        let out_of_bounds = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reader.get_color_mappings(0, &mut Vec::new())
        }));
        assert!(out_of_bounds.is_err());
        drop(reader);
        std::fs::remove_file(path).unwrap();
    }
}
