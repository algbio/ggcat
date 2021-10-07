use crate::KEEP_FILES;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::cmp::max;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::mem::{swap, transmute};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub type ColorIndexType = u32;

const COLORS_VECS_DEFAULT_SIZE: usize = 1024 * 1024;

pub struct ColorsStorage {
    colors: DashMap<&'static [ColorIndexType], ColorIndexType>,
    colors_vecs: Mutex<Vec<Vec<ColorIndexType>>>,
    color_index: AtomicU64,
    colors_threadvecs: Vec<UnsafeCell<Vec<ColorIndexType>>>,
}

unsafe impl Sync for ColorsStorage {}

impl ColorsStorage {
    pub fn new() -> Self {
        let mut colors_threadvecs = Vec::new();
        for i in 0..max(1, rayon::current_num_threads()) {
            colors_threadvecs.push(UnsafeCell::new(Vec::with_capacity(
                COLORS_VECS_DEFAULT_SIZE,
            )));
        }

        Self {
            colors: DashMap::new(),
            colors_vecs: Mutex::new(vec![]),
            color_index: AtomicU64::new(0),
            colors_threadvecs,
        }
    }

    fn add_colors(&self, colors: &[ColorIndexType]) -> &'static [ColorIndexType] {
        let vec = unsafe {
            &mut *self.colors_threadvecs[rayon::current_thread_index().unwrap_or(0)].get()
        };

        if vec.capacity() - vec.len() < colors.len() && (vec.len() > 0) {
            let mut new_vec = Vec::with_capacity(max(COLORS_VECS_DEFAULT_SIZE, colors.len()));

            swap(vec, &mut new_vec);
            self.colors_vecs.lock().push(new_vec);
        }

        let len = vec.len();
        vec.extend_from_slice(colors);
        let result: &'static [ColorIndexType] = unsafe { transmute(&vec[len..]) };
        result
    }

    pub fn get_id(&self, colors: &[ColorIndexType]) -> ColorIndexType {
        match self.colors.get(colors) {
            None => {
                let color = self.color_index.fetch_add(1, Ordering::Relaxed) as ColorIndexType;
                let colors = self.add_colors(colors);
                self.colors.insert(colors, color);
                color
            }
            Some(id) => *id,
        }
    }

    pub fn write_to_file(&self, file: impl AsRef<Path>) {
        let res = self
            .colors
            .iter()
            .map(|x| (*x.key(), *x.value()))
            .collect::<Vec<_>>();

        let storage = serde_json::to_vec_pretty(&res).unwrap();
        File::create(file)
            .unwrap()
            .write_all(storage.as_slice())
            .unwrap();
    }

    pub fn print_stats(&self) {
        let vec_count = self.colors_vecs.lock().len() + self.colors_threadvecs.len();
        println!(
            "Color vectors count: {} sz: {}",
            vec_count,
            vec_count * COLORS_VECS_DEFAULT_SIZE
        );
        println!(
            "Subsets count: {}",
            self.color_index.load(Ordering::Relaxed)
        );
    }
}
