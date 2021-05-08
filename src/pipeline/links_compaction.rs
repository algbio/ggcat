use crate::binary_writer::{BinaryWriter, StorageMode};
use crate::multi_thread_buckets::{MultiThreadBuckets, BucketsThreadDispatcher};
use crate::pipeline::Pipeline;
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::io::Cursor;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use crate::unitig_link::{UnitigLink, Direction};
use crate::smart_bucket_sort::{SortKey, smart_radix_sort};
use crate::vec_slice::VecSlice;

impl Pipeline {
    pub fn links_compaction(
        links_inputs: Vec<PathBuf>,
        output_dir: impl AsRef<Path>,
        buckets_count: usize,
        elab_index: usize,
    ) -> Vec<PathBuf> {
        let totsum = AtomicU64::new(0);

        let mut links_buckets = MultiThreadBuckets::<BinaryWriter>::new(
            buckets_count,
            &(
                output_dir
                    .as_ref()
                    .to_path_buf()
                    .join(format!("linksi{}", elab_index)),
                StorageMode::Plain,
            ),
        );

        links_inputs
            .par_iter()
            .enumerate()
            .for_each(|(index, input)| {
                let mut links_tmp = BucketsThreadDispatcher::new(65536, &links_buckets);

                let file = filebuffer::FileBuffer::open(input).unwrap();
                let mut vec = Vec::new();

                let mut reader = Cursor::new(file.deref());
                let mut unitigs_vec = Vec::new();

                while let Some(entry) = UnitigLink::read_from(&mut reader, &mut unitigs_vec) {
                    vec.push(entry);
                }

                struct Compare {}
                impl SortKey<UnitigLink> for Compare {
                    fn get(value: &UnitigLink) -> u64 {
                        value.entry
                    }
                }

                smart_radix_sort::<_, Compare, false>(&mut vec[..], 64 - 8);

                let mut links = 0;
                let mut not_links = 0;

                for x in vec.group_by(|a, b| a.entry == b.entry) {
                    if x.len() == 2 {
                        links += 1;
                        links_tmp.add_element(
                            0 as usize,
                            &mut unitigs_vec,
                            UnitigLink {

                                // is_forward: true,
                                // entry: x[0].entry2,
                                // bucket2: x[1].bucket2,
                                // entry2: x[1].entry2,
                                // trash: Default::default(),
                                entry: 0,
                                direction: Direction::Forward,
                                entries: VecSlice::new(0, 0)
                            },
                        );
                        // links_tmp.add_element(
                        //     x[1].bucket2 as usize,
                        //     UnitigLink {
                        //         is_forward: true,
                        //         entry: x[1].entry2,
                        //         bucket2: x[0].bucket2,
                        //         entry2: x[0].entry2,
                        //         trash: Default::default(),
                        //     },
                        // );
                    } else {
                        not_links += 1;
                    }
                    assert!(x.len() <= 2);
                }
                println!("Done {} {}/{}!", index, links, not_links);
                totsum.fetch_add(links, Ordering::Relaxed);
                links_tmp.finalize(&unitigs_vec);
            });
        println!("Remaining: {}", totsum.load(Ordering::Relaxed));
        links_buckets.finalize()
    }
}
