use crate::colors::ColorIndexType;
use crate::io::chunks_writer::ChunksWriter;
use crossbeam::channel::*;
use crossbeam::queue::*;
use parking_lot::{Mutex, RwLock, RwLockWriteGuard};
use std::cell::UnsafeCell;
use std::cmp::max;
use std::mem::{swap, take};
use std::ops::{Deref, DerefMut, Range};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct AsyncBuffer<T, P: ChunksWriter<TargetData = T>> {
    data: Option<UnsafeCell<Box<[T]>>>,
    position: AtomicUsize,
    reference: *const AsyncSliceQueue<T, P>,
}

impl<T, P: ChunksWriter<TargetData = T>> AsyncBuffer<T, P> {
    #[allow(clippy::mut_from_ref)]
    fn get_buffer(&self) -> &mut [T] {
        unsafe { (*(self.data.as_ref().unwrap().get())).deref_mut() }
    }
}

struct SliceReference<T, P: ChunksWriter<TargetData = T>> {
    buffer: Arc<AsyncBuffer<T, P>>,
    slice: Range<usize>,
}

impl<T, P: ChunksWriter<TargetData = T>> Drop for AsyncBuffer<T, P> {
    fn drop(&mut self) {
        unsafe {
            (&*self.reference)
                .available_buffers
                .push(self.data.take().unwrap().into_inner());
        }
    }
}

pub struct AsyncSliceQueue<T, P: ChunksWriter<TargetData = T>> {
    chunks_buffers_pool: (
        Sender<Vec<SliceReference<T, P>>>,
        Receiver<Vec<SliceReference<T, P>>>,
    ),
    available_buffers: SegQueue<Box<[T]>>,
    slices_queue: SegQueue<(ColorIndexType, Vec<SliceReference<T, P>>)>,
    push_lock: Mutex<(u64, (ColorIndexType, Vec<SliceReference<T, P>>))>,
    current_slice: RwLock<Option<Arc<AsyncBuffer<T, P>>>>,
    buffer_min_size: usize,
    pub async_processor: P,
}

unsafe impl<T, P: ChunksWriter<TargetData = T>> Sync for AsyncSliceQueue<T, P> {}

impl<T: Copy, P: ChunksWriter<TargetData = T>> AsyncSliceQueue<T, P> {
    pub fn new(
        buffer_min_size: usize,
        max_slices_buffers_count: usize,
        slices_buffers_size: usize,
        async_processor: P,
    ) -> Self {
        let (sender, receiver) = crossbeam::channel::bounded(max_slices_buffers_count);

        for _ in 1..max_slices_buffers_count {
            sender.send(Vec::with_capacity(slices_buffers_size));
        }

        AsyncSliceQueue {
            chunks_buffers_pool: (sender, receiver),
            available_buffers: SegQueue::new(),
            slices_queue: SegQueue::new(),
            push_lock: Mutex::new((0, (0, Vec::with_capacity(slices_buffers_size)))),
            current_slice: RwLock::new(None),
            buffer_min_size,
            async_processor,
        }
    }

    pub fn get_counter(&self) -> u64 {
        self.push_lock.lock().0
    }

    fn alloc_buffer(&self, min_length: usize) -> Arc<AsyncBuffer<T, P>> {
        let buffer = self.available_buffers.pop().unwrap_or_else(|| unsafe {
            Box::new_zeroed_slice(max(min_length, self.buffer_min_size)).assume_init()
        });

        Arc::new(AsyncBuffer {
            data: Some(UnsafeCell::new(buffer)),
            position: AtomicUsize::new(0),
            reference: self as *const _,
        })
    }

    pub fn add_data(&self, data: &[T]) -> u64 {
        let mut current_slice = self.current_slice.read();

        loop {
            if let Some(read) = current_slice.deref() {
                let my_read = read.clone();
                drop(current_slice);

                let position = my_read.position.fetch_add(data.len(), Ordering::Relaxed);
                let slice = my_read.get_buffer();

                if position + data.len() <= slice.len() {
                    let slice_range = position..(position + data.len());

                    slice[slice_range.clone()].copy_from_slice(data);

                    let mut push_lock = self.push_lock.lock();

                    if push_lock.1 .1.len() == push_lock.1 .1.capacity() {
                        loop {
                            drop(push_lock);
                            while let Some((color_start, mut slice_array)) = self.slices_queue.pop()
                            {
                                let mut tmp_buffer = self.async_processor.start_processing();
                                for slice in slice_array.drain(..) {
                                    let current_slice = &slice.buffer.get_buffer()[slice.slice];
                                    self.async_processor
                                        .flush_data(&mut tmp_buffer, current_slice);
                                }
                                self.async_processor
                                    .end_processing(tmp_buffer, color_start, 1);
                                self.chunks_buffers_pool.0.send(slice_array);
                            }

                            push_lock = self.push_lock.lock();

                            if push_lock.1 .1.len() == push_lock.1 .1.capacity() {
                                let mut tmp_buffer = (
                                    push_lock.0 as ColorIndexType,
                                    match self.chunks_buffers_pool.1.try_recv() {
                                        Ok(buffer) => buffer,
                                        Err(_) => {
                                            continue;
                                        }
                                    },
                                );
                                tmp_buffer.1.clear();

                                swap(&mut push_lock.1, &mut tmp_buffer);

                                self.slices_queue.push(tmp_buffer);
                            }
                            break;
                        }
                    }

                    push_lock.1 .1.push(SliceReference {
                        buffer: my_read,
                        slice: slice_range,
                    });
                    let unique_index = push_lock.0;
                    push_lock.0 += 1;

                    drop(push_lock);
                    return unique_index;
                }
            } else {
                drop(current_slice);
            }

            let mut current_slice_wr = self.current_slice.write();
            *current_slice_wr = Some(self.alloc_buffer(data.len()));
            current_slice = RwLockWriteGuard::downgrade(current_slice_wr);
        }
    }

    pub fn finish(self) -> P {
        let mut push_lock = self.push_lock.lock();
        self.slices_queue.push(take(&mut push_lock.1));

        while let Some((color, slice_array)) = self.slices_queue.pop() {
            let mut tmp_data = self.async_processor.start_processing();

            for slice in slice_array {
                let current_slice = &slice.buffer.get_buffer()[slice.slice];
                self.async_processor
                    .flush_data(&mut tmp_data, current_slice);
            }

            self.async_processor.end_processing(tmp_data, color, 1);
        }
        self.async_processor
    }
}
