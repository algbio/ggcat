use crate::instr_span::InstrMetadata;
use crate::{InstrSpan, EVENTS_LIST, EVENTS_SET, SPANS_LIST};
use dashmap::DashMap;
use libc::{clockid_t, timespec};
use papi::events_set::EventsSet;
use std::cell::RefCell;
use std::cmp::max;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::os::raw::c_longlong;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Record};
use tracing::{Event, Id, Metadata, Subscriber};

struct PartialSpanInfo {
    ref_count: usize,
    start: Option<Duration>,
    end: Option<Duration>,
    running_time: Duration,
    clock_id: clockid_t,
    user_time: Duration,
    counters_sum: Vec<u64>,
    counters_temp_start: Vec<c_longlong>,
    counters_temp_exit: Vec<c_longlong>,
    enter_count: usize,
    temp_start: Duration,
    temp_user_start: Duration,
    parameters: Vec<(String, i128)>,

    curr_own_memory: usize,
    curr_own_memory_items: usize,

    max_own_memory: usize,
    max_own_memory_items: usize,

    metadata: &'static Metadata<'static>,
}

struct MemoryInfo {
    items_created: usize,
    bytes_allocated: usize,
    bytes_freed: usize,

    maximum_bytes: usize,
    current_bytes: usize,
    items_count: usize,
}

impl MemoryInfo {
    const fn new() -> Self {
        Self {
            items_created: 0,
            bytes_allocated: 0,
            bytes_freed: 0,
            maximum_bytes: 0,
            current_bytes: 0,
            items_count: 0,
        }
    }
}

pub(crate) struct InstrSubscriber {
    time: Instant,
    spans_map: DashMap<Id, PartialSpanInfo>,
    memory_map: DashMap<&'static str, MemoryInfo>,
    pointers_map: DashMap<usize, ((&'static str, Id), usize)>,
}

impl InstrSubscriber {
    pub fn new() -> Self {
        Self {
            time: Instant::now(),
            spans_map: DashMap::new(),
            memory_map: DashMap::new(),
            pointers_map: DashMap::new(),
        }
    }
}

static SPAN_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CLOCK_ID: clockid_t = {
        let mut clock_id = 0;
        unsafe {
            let retval = libc::pthread_getcpuclockid(
                libc::pthread_self(),
                &mut clock_id as *mut clockid_t,
            );
            assert_eq!(retval, 0);
        }
        clock_id
    };

    static LOCAL_SPANS_STACK: RefCell<Vec<(&'static str, Id)>> = RefCell::new(vec![]);
}

fn get_current_cpu_time() -> Duration {
    let mut timespec = timespec {
        tv_nsec: 0,
        tv_sec: 0,
    };
    unsafe {
        CLOCK_ID.with(|id| {
            libc::clock_gettime(*id, &mut timespec as *mut timespec);
        })
    }
    Duration::new(timespec.tv_sec as u64, timespec.tv_nsec as u32)
}

struct Visitor {
    parameters: Vec<(String, i128)>,
}

impl Visit for Visitor {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.parameters.push((field.to_string(), value as i128));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.parameters.push((field.to_string(), value as i128));
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.parameters.push((field.to_string(), value as i128));
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.parameters.push((field.to_string(), value as i128));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        panic!("Not supported!")
    }
}

impl InstrSubscriber {
    pub(crate) fn notify_memory_alloc(&self, ptr: usize, size: usize) {
        if let Some(span_info) =
            LOCAL_SPANS_STACK.with(|s| s.try_borrow().ok().map(|s| s.last().cloned()).flatten())
        {
            self.pointers_map.insert(ptr, (span_info.clone(), size));
            let mut mmap = self
                .memory_map
                .entry(span_info.0)
                .or_insert(MemoryInfo::new());
            mmap.bytes_allocated += size;
            mmap.current_bytes += size;
            mmap.items_created += 1;
            mmap.items_count += 1;
            mmap.maximum_bytes = max(mmap.maximum_bytes, mmap.current_bytes);
            drop(mmap);

            let mut psinfo = self.spans_map.get_mut(&span_info.1).unwrap();
            psinfo.curr_own_memory += size;
            psinfo.curr_own_memory_items += 1;
            if psinfo.max_own_memory < psinfo.curr_own_memory {
                psinfo.max_own_memory = psinfo.curr_own_memory;
                psinfo.max_own_memory_items = psinfo.curr_own_memory_items;
            }
        }
    }

    pub(crate) fn notify_memory_dealloc(&self, ptr: usize) {
        if let Some((_, (span_info, size))) = self.pointers_map.remove(&ptr) {
            let mut mmap = self.memory_map.get_mut(span_info.0).unwrap();
            mmap.bytes_freed += size;
            mmap.current_bytes -= size;
            mmap.items_count -= 1;
            drop(mmap);

            if let Some(mut psinfo) = self.spans_map.get_mut(&span_info.1) {
                psinfo.curr_own_memory -= size;
                psinfo.curr_own_memory_items -= 1;
            }
        }
    }

    fn init_local_eventset() {
        EVENTS_SET.with(|evt_set| {
            let mut set = evt_set.borrow_mut();
            if set.is_none() {
                *set = Some(EventsSet::new(&EVENTS_LIST.read()).unwrap());
                set.as_mut().unwrap().start().unwrap();
            }
        });
    }
}

impl Subscriber for InstrSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &Attributes<'_>) -> Id {
        let span_id = SPAN_ID.fetch_add(1, Ordering::Relaxed);

        let id = Id::from_u64(span_id);

        Self::init_local_eventset();

        let events_count = EVENTS_SET.with(|set| set.borrow().as_ref().unwrap().len());

        let mut visitor = Visitor { parameters: vec![] };
        span.record(&mut visitor);

        self.spans_map.insert(
            id.clone(),
            PartialSpanInfo {
                ref_count: 1,
                start: None,
                end: None,
                running_time: Duration::ZERO,
                clock_id: 0,
                user_time: Duration::ZERO,
                counters_sum: vec![0; events_count],
                counters_temp_start: vec![0; events_count],
                counters_temp_exit: vec![0; events_count],
                enter_count: 0,
                temp_start: Duration::ZERO,
                temp_user_start: Duration::ZERO,
                parameters: visitor.parameters,
                curr_own_memory: 0,
                curr_own_memory_items: 0,
                max_own_memory: 0,
                max_own_memory_items: 0,
                metadata: span.metadata(),
            },
        );

        id
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &Event<'_>) {}

    fn enter(&self, span: &Id) {
        Self::init_local_eventset();
        EVENTS_SET.with(|evt_set| {
            let mut span_info = self.spans_map.get_mut(span).unwrap();

            span_info.temp_start = self.time.elapsed();
            if span_info.start.is_none() {
                span_info.start = Some(span_info.temp_start);
            }

            span_info.temp_user_start = get_current_cpu_time();
            span_info.clock_id = CLOCK_ID.with(|cid| *cid);

            span_info.enter_count += 1;

            LOCAL_SPANS_STACK.with(|lss| {
                lss.borrow_mut()
                    .push((span_info.metadata.name(), span.clone()));
            });

            evt_set
                .borrow()
                .as_ref()
                .unwrap()
                .read_into(span_info.counters_temp_start.as_mut_slice())
                .unwrap();
        });
    }

    fn exit(&self, span: &Id) {
        EVENTS_SET.with(|evt_set| {
            let mut span_info = self.spans_map.get_mut(span).unwrap();
            let span_info = span_info.deref_mut();

            let exit_time = self.time.elapsed();

            span_info.end = Some(exit_time);
            span_info.running_time += exit_time - span_info.temp_start;

            assert_eq!(CLOCK_ID.with(|cid| *cid), span_info.clock_id);

            let user_exit_time = get_current_cpu_time();
            span_info.user_time += user_exit_time - span_info.temp_user_start;

            evt_set
                .borrow()
                .as_ref()
                .unwrap()
                .read_into(span_info.counters_temp_exit.as_mut_slice())
                .unwrap();

            LOCAL_SPANS_STACK.with(|lss| {
                lss.borrow_mut().pop();
            });

            for cidx in 0..span_info.counters_sum.len() {
                span_info.counters_sum[cidx] += (span_info.counters_temp_exit[cidx]
                    - span_info.counters_temp_start[cidx])
                    as u64;
            }
        });
    }

    fn clone_span(&self, id: &Id) -> Id {
        self.spans_map.get_mut(id).unwrap().ref_count += 1;
        id.clone()
    }

    fn drop_span(&self, id: Id) {
        let mut entry = self.spans_map.get_mut(&id).unwrap();

        entry.ref_count -= 1;
        if entry.ref_count == 0 {
            drop(entry);
            let element = self.spans_map.remove(&id).unwrap().1;

            let meta_mem_info = self.memory_map.get_mut(element.metadata.name());

            static EMPTY_MEMINFO: MemoryInfo = MemoryInfo::new();
            let meta_mem_info_ref = meta_mem_info
                .as_ref()
                .map(|x| x.deref())
                .unwrap_or(&EMPTY_MEMINFO);
            let current_tot_memory = meta_mem_info_ref.current_bytes;
            let current_tot_items = meta_mem_info_ref.items_count;
            let max_memory = meta_mem_info_ref.maximum_bytes;
            drop(meta_mem_info);

            SPANS_LIST.lock().push(InstrSpan {
                thread_id: 0,
                start_time: element.start.unwrap(),
                end_time: element.end.unwrap(),
                executon_time: element.running_time,
                user_time: element.user_time,
                enter_count: element.enter_count,
                counters: element.counters_sum,
                parameters: element.parameters,
                meta: InstrMetadata {
                    name: element.metadata.name().to_string(),
                    target: element.metadata.target().to_string(),
                    module_path: element.metadata.module_path().map(|s| s.to_string()),
                    file: element.metadata.file().map(|s| s.to_string()),
                    line: element.metadata.line(),
                },
                max_own_memory: element.max_own_memory,
                max_own_items: element.max_own_memory_items,
                current_tot_memory,
                current_tot_items,
                max_memory,
            })
        }
    }
}
