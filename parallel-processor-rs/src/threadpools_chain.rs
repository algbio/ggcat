use crate::stats_logger::StatRaiiCounter;
use crossbeam::channel::*;
use crossbeam::queue::*;
use crossbeam::thread;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub trait ThreadChainObject: Send + Sync {
    type InitData: Send + Sync;

    fn initialize(params: &Self::InitData) -> Self;
    fn reset(&mut self);
}

impl<T: Sync + Send + Default> ThreadChainObject for T {
    type InitData = ();

    fn initialize(_params: &()) -> Self {
        Default::default()
    }

    fn reset(&mut self) {
        *self = Default::default()
    }
}

pub struct ThreadPoolDefinition<
    'a,
    C: Sync + Send,
    OS: ThreadChainObject,
    OR: ThreadChainObject,
    F: Fn(&C, ObjectsPoolManager<OS, OR>),
> {
    context: &'a C,
    obj_init_data: OS::InitData,
    name: String,
    max_threads_count: usize,
    current_thread_count: &'a AtomicUsize,
    max_out_queue_size: usize,
    function: F,
    _phantom: PhantomData<OR>,
}

impl<
        'a,
        C: Sync + Send,
        OS: ThreadChainObject,
        OR: ThreadChainObject,
        F: Fn(&C, ObjectsPoolManager<OS, OR>),
    > ThreadPoolDefinition<'a, C, OS, OR, F>
{
    pub fn new(
        context: &'a C,
        obj_init_data: OS::InitData,
        name: String,
        max_threads_count: usize,
        current_thread_count: &'a AtomicUsize,
        max_out_queue_size: usize,
        function: F,
    ) -> Self {
        ThreadPoolDefinition {
            context,
            obj_init_data,
            name,
            max_threads_count,
            current_thread_count,
            max_out_queue_size,
            function,
            _phantom: PhantomData,
        }
    }
}

pub struct ObjectsPoolManager<'a, OS: ThreadChainObject, OR: ThreadChainObject> {
    init_params: &'a OS::InitData,
    objects_pool: &'a SegQueue<OS>,
    recv_objects_pool: &'a SegQueue<OR>,
    sender: &'a Sender<OS>,
    receiver: &'a Receiver<OR>,
    queue_waiting_name: &'static str,
    thread_index: usize,
    concurrency_limit: &'a AtomicUsize,
}
impl<'a, OS: ThreadChainObject, OR: ThreadChainObject> ObjectsPoolManager<'a, OS, OR> {
    pub fn allocate(&self) -> OS {
        self.objects_pool
            .pop()
            .unwrap_or_else(|| OS::initialize(self.init_params))
    }

    pub fn deallocate(&self, obj: OS) {
        self.objects_pool.push(obj);
    }

    pub fn send(&self, object: OS) {
        let _stat_raii = StatRaiiCounter::create("THREADS_BUSY_WAITING_ON_SEND");
        update_stat!("WAITING_THREADS", 1.0, StatMode::Sum);
        update_stat!(
            self.queue_waiting_name,
            self.sender.len() as f64,
            StatMode::Replace
        );
        self.sender.send(object).unwrap();
        update_stat!(
            self.queue_waiting_name,
            self.sender.len() as f64,
            StatMode::Replace
        );
        update_stat!("WAITING_THREADS", -1.0, StatMode::Sum);
    }
    pub fn return_obj(&self, mut object: OR) {
        object.reset();
        self.recv_objects_pool.push(object);
    }
    pub fn recv_obj(&self) -> Option<OR> {
        if self.concurrency_limit.load(Ordering::Relaxed) <= self.thread_index {
            return None;
        }

        let _stat_raii = StatRaiiCounter::create("THREADS_BUSY_WAITING_ON_RECV");
        update_stat!("WAITING_THREADS_RECV", 1.0, StatMode::Sum);
        update_stat!(
            self.queue_waiting_name,
            self.sender.len() as f64,
            StatMode::Replace
        );

        let recv = self.receiver.recv().ok();
        update_stat!(
            self.queue_waiting_name,
            self.sender.len() as f64,
            StatMode::Replace
        );
        update_stat!("WAITING_THREADS_RECV", -1.0, StatMode::Sum);
        recv
    }
    pub fn get_queue_len(&self) -> usize {
        self.receiver.len()
    }
}

pub struct ThreadPoolsChain;
impl ThreadPoolsChain {
    pub fn run_single<
        C: Sync + Send,
        OI: ThreadChainObject<InitData = ()>,
        OO: ThreadChainObject,
        F: Fn(&C, ObjectsPoolManager<OO, OI>) + Sync,
    >(
        input_data: Vec<OI>,
        first: ThreadPoolDefinition<C, OO, OI, F>,
    ) -> Vec<OO> {
        let mut output = Vec::new();

        let waiting_name_1 = Box::new(format!("{}{}", first.name, "_WAITING_QUEUE_SIZE"));
        let waiting_name_1 = waiting_name_1.as_str();

        thread::scope(|s| {
            let mut threads = Vec::with_capacity(first.max_threads_count);

            let (in_sender, in_receiver) = bounded(input_data.len());
            let in_sender_arc = Arc::new(in_sender);
            let in_receiver_arc = Arc::new(in_receiver);
            let in_pool_arc = Arc::new(SegQueue::new());

            let (out_sender, out_receiver) = bounded(first.max_out_queue_size);
            let out_sender_arc = Arc::new(out_sender);
            let out_receiver_arc = Arc::new(out_receiver);
            let out_pool_arc = Arc::new(SegQueue::new());

            for thread_index in 0..first.max_threads_count {
                let in_pool_arc = in_pool_arc.clone();
                let in_receiver_arc = in_receiver_arc.clone();
                let in_sender_weak = Arc::downgrade(&in_sender_arc);

                let out_sender_arc = out_sender_arc.clone();
                let out_pool_arc = out_pool_arc.clone();

                let first = &first;

                threads.push(s.builder().name(first.name.clone()).spawn(move |_| {
                    while in_receiver_arc.len() > 0 || in_sender_weak.strong_count() > 0 {
                        if first.current_thread_count.load(Ordering::Relaxed) <= thread_index {
                            std::thread::sleep(Duration::from_millis(500));
                            continue;
                        }
                        let _stat_raii = StatRaiiCounter::create("THREADS_RUNNING_IN_SINGLE_MODE");
                        (first.function)(
                            first.context,
                            ObjectsPoolManager {
                                init_params: &first.obj_init_data,
                                objects_pool: out_pool_arc.as_ref(),
                                recv_objects_pool: in_pool_arc.as_ref(),
                                sender: out_sender_arc.as_ref(),
                                receiver: in_receiver_arc.as_ref(),
                                queue_waiting_name: unsafe { core::mem::transmute(waiting_name_1) },
                                thread_index,
                                concurrency_limit: first.current_thread_count,
                            },
                        )
                    }
                }));
            }
            drop(out_sender_arc);

            for el in input_data {
                in_sender_arc.send(el).unwrap();
            }
            drop(in_sender_arc);

            while let Ok(data) = out_receiver_arc.recv() {
                output.push(data);
                continue;
            }
        })
        .unwrap();
        output
    }

    pub fn run_double<
        C1: Sync + Send,
        C2: Sync + Send,
        OI: ThreadChainObject<InitData = ()>,
        O1: ThreadChainObject,
        O2: ThreadChainObject,
        F1: Fn(&C1, ObjectsPoolManager<O1, OI>) + Sync, //ThreadChainFunction<C1, O1, OI> + Sync,
        F2: Fn(&C2, ObjectsPoolManager<O2, O1>) + Sync,
    >(
        input_data: Vec<OI>,
        first: ThreadPoolDefinition<C1, O1, OI, F1>,
        second: ThreadPoolDefinition<C2, O2, O1, F2>,
    ) -> Vec<O2> {
        let mut output = Vec::new();

        let waiting_name_1 = Box::new(format!("{}{}", first.name, "_WAITING_QUEUE_SIZE"));
        let waiting_name_2 = Box::new(format!("{}{}", second.name, "_WAITING_QUEUE_SIZE"));

        let waiting_name_1 = waiting_name_1.as_str();
        let waiting_name_2 = waiting_name_2.as_str();

        thread::scope(|s| {
            let mut first_threads = Vec::with_capacity(first.max_threads_count);
            let mut second_threads = Vec::with_capacity(second.max_threads_count);

            let (in_sender, in_receiver) = bounded(input_data.len());
            let in_sender_arc = Arc::new(in_sender);
            let in_receiver_arc = Arc::new(in_receiver);
            let in_pool_arc = Arc::new(SegQueue::new());

            let (f_sender, f_receiver) = bounded(first.max_out_queue_size);
            let f_sender_arc = Arc::new(f_sender);
            let f_receiver_arc = Arc::new(f_receiver);
            let f_pool_arc = Arc::new(SegQueue::new());

            let (s_sender, s_receiver) = bounded(second.max_out_queue_size);
            let s_sender_arc = Arc::new(s_sender);
            let s_pool_arc = Arc::new(SegQueue::new());

            for thread_index in 0..first.max_threads_count {
                let in_pool_arc = in_pool_arc.clone();
                let in_receiver_arc = in_receiver_arc.clone();
                let in_sender_weak = Arc::downgrade(&in_sender_arc);

                let f_sender_arc = f_sender_arc.clone();
                let f_pool_arc = f_pool_arc.clone();

                let first = &first;

                first_threads.push(s.builder().name(first.name.clone()).spawn(move |_| {
                    while in_receiver_arc.len() > 0 || in_sender_weak.strong_count() > 0 {
                        if first.current_thread_count.load(Ordering::Relaxed) <= thread_index {
                            std::thread::sleep(Duration::from_millis(100));
                            continue;
                        }
                        let _stat_raii = StatRaiiCounter::create("THREADS_RUNNING_IN_READER_MODE");
                        (first.function)(
                            first.context,
                            ObjectsPoolManager {
                                init_params: &first.obj_init_data,
                                objects_pool: f_pool_arc.as_ref(),
                                recv_objects_pool: in_pool_arc.as_ref(),
                                sender: f_sender_arc.as_ref(),
                                receiver: in_receiver_arc.as_ref(),
                                queue_waiting_name: unsafe { core::mem::transmute(waiting_name_1) },
                                thread_index,
                                concurrency_limit: first.current_thread_count,
                            },
                        )
                    }
                }));
            }

            for thread_index in 0..second.max_threads_count {
                let f_receiver_arc = f_receiver_arc.clone();
                let f_pool_arc = f_pool_arc.clone();
                let f_sender_weak = Arc::downgrade(&f_sender_arc);

                let s_sender_arc = s_sender_arc.clone();
                let s_pool_arc = s_pool_arc.clone();

                let second = &second;
                second_threads.push(s.builder().name(first.name.clone()).spawn(move |_| {
                    while f_receiver_arc.len() > 0 || f_sender_weak.strong_count() > 0 {
                        if second.current_thread_count.load(Ordering::Relaxed) <= thread_index {
                            std::thread::sleep(Duration::from_millis(500));
                            continue;
                        }
                        let _stat_raii = StatRaiiCounter::create("THREADS_RUNNING_IN_WRITER_MODE");
                        (second.function)(
                            second.context,
                            ObjectsPoolManager {
                                init_params: &second.obj_init_data,
                                objects_pool: s_pool_arc.as_ref(),
                                recv_objects_pool: f_pool_arc.as_ref(),
                                sender: s_sender_arc.as_ref(),
                                receiver: f_receiver_arc.as_ref(),
                                queue_waiting_name: unsafe { core::mem::transmute(waiting_name_2) },
                                thread_index,
                                concurrency_limit: second.current_thread_count,
                            },
                        )
                    }
                }));
            }
            drop(f_sender_arc);
            drop(s_sender_arc);

            for el in input_data {
                in_sender_arc.send(el).unwrap();
            }
            drop(in_sender_arc);

            while let Ok(data) = s_receiver.recv() {
                output.push(data);
                continue;
            }
        })
        .unwrap();
        output
    }
}
