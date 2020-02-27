use std::alloc::{alloc_zeroed, Layout};
use std::slice::from_raw_parts_mut;

pub struct BloomFilter {
    map: Box<[u8]>,
    pub k: usize
}

impl BloomFilter {
    pub fn new(size: usize, k: usize) -> BloomFilter {
        BloomFilter {
            map: unsafe {
                Box::from_raw(from_raw_parts_mut(alloc_zeroed(Layout::from_size_align(size, 1).unwrap()), size))
            },
            k
        }
    }

    #[inline(always)]
    pub fn get_cell(&mut self, mut cell: usize) -> bool {
        let shift = (cell % 8) as u8;
        let map_cell = &mut self.map[cell / 8];
        let value = (*map_cell >> shift) & 0b1;
        value != 0
    }

    #[inline(always)]
    pub fn increment_cell(&mut self, mut cell: usize) -> bool {
//        println!("{}", cell);
//        let res = self.map[cell] == 1;
//        self.map[cell] = 1;
//        res
        let shift = (cell % 8) as u8;
        let map_cell = &mut self.map[cell / 8];
        let value = (*map_cell >> shift) & 0b1;
        if value != 0 {
            true
        }
        else {
            *map_cell |= 1 << shift;
            false
        }
//        let shift = ((cell % 4) * 2) as u8;
//        let map_cell = &mut self.map[(cell as usize) / 4];
//        let value = (*map_cell >> shift) & 0b11;
//        if value == 0b11 {
//            false
//        }
//        else {
//            *map_cell = (*map_cell & !(0b11 << shift)) | ((value + 1) << shift);
//            true
//        }
    }
}