use std::collections::{BTreeMap, HashMap};
use std::fs::File;

use std::path::{Path, PathBuf};

use std::sync::Arc;

use crate::memory_fs::file::flush::*;

use parking_lot::Mutex;

use crate::memory_data_size::MemoryDataSize;
use crate::memory_fs::allocator::CHUNKS_ALLOCATOR;
use crate::memory_fs::file::internal::{MemoryFileInternal, SWAPPABLE_FILES};
use nightly_quirks::utils::NightlyUtils;

pub const O_DIRECT: i32 = 0x4000;

#[macro_use]
pub mod allocator;
pub mod file;
pub mod flushable_buffer;

static mut FILES_FLUSH_HASH_MAP: Option<Mutex<HashMap<PathBuf, Vec<Arc<(PathBuf, Mutex<File>)>>>>> =
    None;

pub struct MemoryFs;

impl MemoryFs {
    pub fn init(
        memory_size: MemoryDataSize,
        flush_queue_size: usize,
        threads_count: usize,
        min_chunks_count: usize,
    ) {
        unsafe {
            let chunk_size = (memory_size / (min_chunks_count as f64)).as_bytes();

            let mut suggested_chunk_size_log = 1;

            while (1 << (suggested_chunk_size_log + 1)) <= chunk_size {
                suggested_chunk_size_log += 1;
            }

            CHUNKS_ALLOCATOR.initialize(memory_size, suggested_chunk_size_log, min_chunks_count);
            FILES_FLUSH_HASH_MAP = Some(Mutex::new(HashMap::with_capacity(8192)));
            GlobalFlush::init(flush_queue_size, threads_count);
        }
    }

    pub fn remove_file(file: impl AsRef<Path>, remove_fs: bool) -> Result<(), ()> {
        if MemoryFileInternal::delete(file, remove_fs) {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn get_file_size(file: impl AsRef<Path>) -> Option<usize> {
        MemoryFileInternal::retrieve_reference(&file)
            .map(|f| f.len())
            .or_else(|| std::fs::metadata(&file).map(|m| m.len() as usize).ok())
    }

    pub fn ensure_flushed(file: impl AsRef<Path>) {
        unsafe {
            FILES_FLUSH_HASH_MAP
                .as_mut()
                .unwrap()
                .lock()
                .remove(&file.as_ref().to_path_buf());
        }
    }

    pub fn flush_all_to_disk() {
        GlobalFlush::flush_to_disk();
    }

    pub fn terminate() {
        GlobalFlush::terminate();
        CHUNKS_ALLOCATOR.deinitialize();
    }

    pub fn reduce_pressure() -> bool {
        // println!("Reducing pressure!");
        let (current, max_size) = GlobalFlush::global_queue_occupation();
        if current * 3 < max_size {
            let mut map_lock = SWAPPABLE_FILES.lock();
            let map_lock = NightlyUtils::mutex_get_or_init(&mut map_lock, || BTreeMap::new());

            let mut file_ref = None;

            for (key, file) in map_lock.iter() {
                if let Some(file) = file.upgrade() {
                    if file.is_memory_preferred() && file.has_flush_pending_chunks() {
                        file_ref = Some((key.clone(), file));
                        break;
                    }
                }
            }

            if let Some((key, file)) = file_ref {
                map_lock.remove(&key);
                drop(map_lock);
                file.change_to_disk_only();
                file.flush_chunks(usize::MAX);
                return true;
            }
        }

        return !GlobalFlush::is_queue_empty();
    }

    // pub fn create_from_joined_files(
    //     files: Vec<Arc<MemoryFileInternal>>,
    // ) -> Arc<MemoryFileInternal> {
    //     let new_file = MemoryFileInternal::new("", MemoryMode::Chunks);
    //     for file in files.into_iter() {
    //         // Guaranteed
    //         if let InternalMemoryMode::Chunks { memory } = unsafe { &mut *(file.data.get()) } {
    //             memory.write();
    //         }
    //     }
    //
    //     Arc::new(new_file)
    // }
    //
    // pub fn create_from_owned_memory(
    //     path: impl AsRef<Path>,
    //     owned_memory: Vec<AllocatedChunk>,
    // ) -> Arc<MemoryFileInternal> {
    //     let mem_file = Arc::new(MemoryFileInternal::new(path.as_ref(), MemoryMode::Chunks));
    //
    //     // Guaranteed
    //     if let InternalMemoryMode::Chunks { memory } = unsafe { &mut *(mem_file.data.get()) } {
    //         let mut data = memory.write();
    //         data.clear();
    //         data.reserve(owned_memory.len());
    //         for el in owned_memory {
    //             data.push(el);
    //         }
    //     }
    //
    //     let mut map = Self::get_map();
    //     map.insert(path.as_ref().to_path_buf(), mem_file.clone());
    //
    //     mem_file
    // }
    //
    // pub fn open(path: impl AsRef<Path>) -> Option<Arc<MemoryFileInternal>> {
    //     let map = Self::get_map();
    //     map.get(&PathBuf::from(path.as_ref())).map(|x| x.clone())
    // }
    //
    // pub fn get_and_remove(path: impl AsRef<Path>) -> Option<Arc<MemoryFileInternal>> {
    //     let mut map = Self::get_map();
    //     map.remove(&PathBuf::from(path.as_ref()))
    // }
    //
    // pub fn flush_async(&self) {
    //     match unsafe { &mut *(self.data.get()) } {
    //         InternalMemoryMode::ChunksFileBuffer {
    //             current_buffer,
    //             file,
    //             ..
    //         } => {
    //             let mut current_buffer = current_buffer.write();
    //             if current_buffer.get_buffer().len() > 0 {
    //                 self.flush_buffer_and_replace(
    //                     &mut current_buffer,
    //                     file.clone(),
    //                     FlushMode::Append,
    //                 );
    //             }
    //         }
    //         _ => {}
    //     }
    // }
    //
    // pub fn extend_with_full_allocated_chunk(&self, chunk: AllocatedChunk) {
    //     match unsafe { &mut *(self.data.get()) } {
    //         InternalMemoryMode::Chunks { memory } => {
    //             let mut write_memory_guard = memory.write();
    //             write_memory_guard.push(chunk);
    //         }
    //         InternalMemoryMode::ChunksFileBuffer { .. } => {
    //             self.write_all(chunk.get());
    //         }
    //     }
    // }
    //
    // }
}

#[cfg(test)]
mod tests {
    use crate::memory_fs::file::flush::GlobalFlush;
    use crate::memory_fs::file::internal::MemoryFileMode;
    use crate::memory_fs::file::reader::FileReader;
    use crate::memory_fs::file::writer::FileWriter;
    use crate::memory_fs::MemoryFs;
    use measurements::Data;
    use rayon::prelude::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    #[test]
    pub fn memory_fs_test() {
        MemoryFs::init(Data::from_mebioctets(100.0 * 1024.0), 1024, 3, 0);
        let mut data = (0..3337).map(|x| (x % 256) as u8).collect::<Vec<u8>>();

        (0..400).into_par_iter().for_each(|i| {
            println!("Writing file {}", i);
            let mut file = FileWriter::create(
                format!("/home/andrea/genome-assembly/test1234/{}.tmp", i),
                MemoryFileMode::PreferMemory,
            );
            for _ in 0..(1024 * 64) {
                file.write(data.as_slice());
            }
            drop(file);
            let mut file2 =
                FileReader::open(format!("/home/andrea/genome-assembly/test1234/{}.tmp", i))
                    .unwrap();

            file2.seek(SeekFrom::Start(17 + 3337 * 12374));
            let mut buffer = [0; 4];
            file2.read_exact(&mut buffer).unwrap();
            assert_eq!(&buffer, &data[17..21]);
        });

        GlobalFlush::flush_to_disk();

        (0..400).into_par_iter().for_each(|i| {
            println!("Reading file {}", i);
            let mut datar = vec![0; 3337];
            let mut file =
                FileReader::open(format!("/home/andrea/genome-assembly/test1234/{}.tmp", i))
                    .unwrap();
            for _ in 0..(1024 * 64) {
                file.read_exact(datar.as_mut_slice()).unwrap();
                assert_eq!(datar, data);
            }
            assert_eq!(file.read(datar.as_mut_slice()).unwrap(), 0);
            println!("Read file {}", i);
        });

        MemoryFs::terminate();
    }
}
