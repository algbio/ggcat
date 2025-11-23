use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use config::MultiplicityCounterType;
use hashes::HashableSequence;
use utils::resize_containers::ResizableVec;

use crate::{
    compressed_read::CompressedRead,
    concurrent::temp_reads::creads_utils::{
        DeserializedRead, MinimizerModeOption, MultiplicityModeOption, WithFixedMultiplicity,
    },
    memstorage::memvarint::{
        compute_memvarint_bytes_count, decode_memvarint_flags, encode_memvarint_flags_with_size,
    },
};

pub mod memvarint;

pub struct ReadMemStorage<
    B: ReadsMemStorageBackend,
    E: Copy,
    MultiplicityMode: MultiplicityModeOption,
    MinimizerMode: MinimizerModeOption,
    const ALIGNED: bool,
> {
    storage: B,
    sequences_count: usize,
    _phantom: PhantomData<(E, MultiplicityMode, MinimizerMode)>,
}

impl<
    B: ReadsMemStorageBackend,
    E: Copy,
    MultiplicityMode: MultiplicityModeOption,
    MinimizerMode: MinimizerModeOption,
    const ALIGNED: bool,
> ReadMemStorage<B, E, MultiplicityMode, MinimizerMode, ALIGNED>
{
    pub fn new(storage: B) -> Self {
        Self {
            storage,
            sequences_count: 0,
            _phantom: PhantomData,
        }
    }

    pub fn encode_read(&mut self, read: &DeserializedRead<E>) {
        memstorage_encode_read::<_, MultiplicityMode, MinimizerMode, ALIGNED>(
            read,
            |needed, reserved| self.storage.reserve_space(needed, reserved),
        );
        self.sequences_count += 1;
    }

    pub fn decode_reads(&self, mut reads_cb: impl FnMut(DeserializedRead<E>)) {
        unsafe {
            let mut current = self.storage.get_data();
            let last = current.add(self.storage.len());

            while current < last {
                let (read, next) =
                    memstorage_decode_read::<_, MultiplicityMode, MinimizerMode, ALIGNED>(current);
                reads_cb(read);
                current = next;
            }
        }
    }

    pub fn sequences_count(&self) -> usize {
        self.sequences_count
    }

    pub fn clear(&mut self) {
        self.storage.clear();
        self.sequences_count = 0;
    }

    pub fn get_storage(&self) -> &B {
        &self.storage
    }
}

pub trait ReadsMemStorageBackend {
    fn reserve_space(&mut self, needed: usize, reserved: usize) -> *mut u8;
    fn get_data(&self) -> *const u8;
    fn len(&self) -> usize;
    fn clear(&mut self);
}

impl<const SIZE: usize> ReadsMemStorageBackend for ResizableVec<u8, SIZE> {
    fn reserve_space(&mut self, needed: usize, reserved: usize) -> *mut u8 {
        self.deref_mut().reserve_space(needed, reserved)
    }

    fn get_data(&self) -> *const u8 {
        self.deref().get_data()
    }

    fn len(&self) -> usize {
        self.deref().len()
    }

    fn clear(&mut self) {
        ResizableVec::clear(self);
    }
}

impl ReadsMemStorageBackend for Vec<u8> {
    fn reserve_space(&mut self, needed: usize, reserved: usize) -> *mut u8 {
        self.reserve(reserved);
        let start = self.len();
        unsafe {
            self.set_len(self.len() + needed);
            self.as_mut_ptr().add(start)
        }
    }

    fn get_data(&self) -> *const u8 {
        self.as_ptr()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn clear(&mut self) {
        self.clear();
    }
}

pub fn memstorage_encode_read<
    E: Copy,
    MultiplicityMode: MultiplicityModeOption,
    MinimizerMode: MinimizerModeOption,
    const ALIGNED: bool,
>(
    read: &DeserializedRead<E>,
    reserve_space_fn: impl FnOnce(usize, usize) -> *mut u8,
) {
    let multiplicity_bytes = if MultiplicityMode::ENABLED {
        if MultiplicityMode::FIXED_SIZE {
            0
        } else {
            compute_memvarint_bytes_count(read.multiplicity as u64)
        }
    } else {
        0
    };

    let bases_count = read.read.bases_count();

    let size_bytes = compute_memvarint_bytes_count(read.read.bases_count() as u64);

    let extra_bytes = size_of::<E>();
    let read_bytes = read.read.get_packed_slice();

    let tot_bytes = if MultiplicityMode::ENABLED {
        if MultiplicityMode::FIXED_SIZE {
            size_of::<MultiplicityCounterType>()
        } else {
            multiplicity_bytes + 1
        }
    } else {
        0
    } + if MinimizerMode::ENABLED { 2 } else { 0 }
        + (size_bytes + 1)
        + extra_bytes
        + if ALIGNED {
            debug_assert_eq!(read.read.start, 0);
            0
        } else {
            1
        }
        + read_bytes.len();

    let mut data_ptr = reserve_space_fn(tot_bytes, tot_bytes + 8);

    unsafe {
        // First write fixed size data

        // Write extra
        std::ptr::write_unaligned(data_ptr as *mut _, read.extra);
        data_ptr = data_ptr.add(size_of::<E>());

        // Write minimizer
        if MinimizerMode::ENABLED {
            std::ptr::write_unaligned(data_ptr as *mut _, read.minimizer_pos);
            data_ptr = data_ptr.add(2);
        }

        // Write multiplicity
        if MultiplicityMode::ENABLED {
            if MultiplicityMode::FIXED_SIZE {
                std::ptr::write_unaligned(data_ptr as *mut _, read.multiplicity);
                data_ptr = data_ptr.add(size_of::<MultiplicityCounterType>());
            } else {
                encode_memvarint_flags_with_size::<false>(
                    multiplicity_bytes,
                    read.multiplicity as u64,
                    0,
                    |data, count| {
                        std::ptr::write_unaligned(data_ptr as *mut _, *data);
                        data_ptr = data_ptr.add(count);
                    },
                );
            }
        }

        // Write start
        if !ALIGNED {
            *data_ptr = read.read.start;
            data_ptr = data_ptr.add(1);
        }

        // Write sequence len and flags
        encode_memvarint_flags_with_size::<true>(
            size_bytes,
            bases_count as u64,
            read.flags,
            |data, count| {
                std::ptr::write_unaligned(data_ptr as *mut _, *data);
                data_ptr = data_ptr.add(count);
            },
        );

        std::ptr::copy_nonoverlapping(read_bytes.as_ptr(), data_ptr, read_bytes.len());
    }
}

pub fn memstorage_decode_reads<
    'a,
    E: Copy,
    MultiplicityMode: MultiplicityModeOption,
    MinimizerMode: MinimizerModeOption,
    const ALIGNED: bool,
>(
    data_ptr: *const u8,
    bytes_count: usize,
    mut reads_cb: impl FnMut(DeserializedRead<'static, E>),
) {
    unsafe {
        let mut current = data_ptr;
        let last = current.add(bytes_count);

        while current < last {
            let (read, next) =
                memstorage_decode_read::<_, MultiplicityMode, MinimizerMode, ALIGNED>(current);
            reads_cb(read);
            current = next as *mut u8;
        }
    }
}

#[inline]
pub fn memstorage_decode_reads_changing<
    'a,
    E: Copy,
    MinimizerMode: MinimizerModeOption,
    const ALIGNED: bool,
>(
    data_ptr: *mut u8,
    bytes_count: usize,
    mut reads_cb: impl FnMut(DeserializedRead<E>, *mut E, *mut MultiplicityCounterType) -> bool,
) -> bool {
    unsafe {
        let mut current = data_ptr;
        let last = current.add(bytes_count);

        while current < last {
            let extra = current as *mut E;

            let multiplicity_offset = size_of::<E>() + if MinimizerMode::ENABLED { 2 } else { 0 };
            let multiplicity = current.add(multiplicity_offset) as *mut MultiplicityCounterType;

            let (read, next) =
                memstorage_decode_read::<_, WithFixedMultiplicity, MinimizerMode, ALIGNED>(current);
            if reads_cb(read, extra, multiplicity) {
                return true;
            }
            current = next as *mut u8;
        }
    }
    false
}

pub unsafe fn memstorage_decode_read<
    'a,
    E: Copy,
    MultiplicityMode: MultiplicityModeOption,
    MinimizerMode: MinimizerModeOption,
    const ALIGNED: bool,
>(
    mut data_ptr: *const u8,
) -> (DeserializedRead<'a, E>, *const u8) {
    unsafe {
        // Read extra
        let extra = std::ptr::read_unaligned(data_ptr as *const E);
        data_ptr = data_ptr.add(size_of::<E>());

        // Read minimizer (if enabled)
        let minimizer_pos = if MinimizerMode::ENABLED {
            let val = std::ptr::read_unaligned(data_ptr as *const u16);
            data_ptr = data_ptr.add(2);
            val
        } else {
            0
        };

        // Read multiplicity (if enabled)
        let multiplicity = if MultiplicityMode::ENABLED {
            if MultiplicityMode::FIXED_SIZE {
                let multiplicity =
                    std::ptr::read_unaligned(data_ptr as *mut MultiplicityCounterType);
                data_ptr = data_ptr.add(size_of::<MultiplicityCounterType>());
                multiplicity
            } else {
                let (consumed, (multiplicity, _)) =
                    decode_memvarint_flags::<false>(&*(data_ptr as *const _));
                data_ptr = data_ptr.add(consumed);
                multiplicity as u32
            }
        } else {
            1 // Default multiplicity
        };

        // Read start (if not aligned)
        let start = if !ALIGNED {
            let start = *data_ptr;
            data_ptr = data_ptr.add(1);
            start
        } else {
            0
        };

        // Read bases_count + flags
        let (consumed, (bases_count, flags)) =
            decode_memvarint_flags::<true>(&*(data_ptr as *const _));
        data_ptr = data_ptr.add(consumed);

        let bases_bytes = (bases_count + start as u64 + 3) / 4;

        (
            DeserializedRead {
                read: CompressedRead {
                    size: bases_count as usize,
                    start,
                    data: data_ptr,
                    _phantom: PhantomData,
                },
                extra,
                multiplicity: multiplicity as _,
                minimizer_pos,
                flags,
                second_bucket: 0, // Not serialized
            },
            data_ptr.add(bases_bytes as usize),
        )
    }
}
