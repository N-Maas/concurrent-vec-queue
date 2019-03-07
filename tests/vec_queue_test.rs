use concurrent_vec_queue::*;

use std::collections::HashSet;
use std::slice::Iter;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

#[test]
fn realloc_mpmc_basic_test() {
    basic_sequential_test_template::<MPMCType<usize, ReallocationPolicy>>(true);
    basic_parallel_test_template::<MPMCType<usize, ReallocationPolicy>>();
    drop_test_template::<MPMCType<DropCounter, ReallocationPolicy>>(true);
}

#[test]
fn realloc_mpmc_single_producer_single_consumer_test() {
    single_producer_single_consumer_test_template::<MPMCType<(usize, usize), ReallocationPolicy>>(
        1, 10000,
    );
}

#[test]
fn realloc_mpmc_multi_producer_multi_consumer_test() {
    multi_producer_multi_consumer_test_template::<MPMCType<(usize, usize), ReallocationPolicy>>(
        1, 1000, 2, 2,
    );
    multi_producer_multi_consumer_test_template::<MPMCType<(usize, usize), ReallocationPolicy>>(
        1, 100, 16, 16,
    );
    multi_producer_multi_consumer_test_template::<MPMCType<(usize, usize), ReallocationPolicy>>(
        1, 250, 128, 128,
    );
    multi_producer_multi_consumer_test_template::<MPMCType<(usize, usize), ReallocationPolicy>>(
        1, 250, 64, 1,
    );
    multi_producer_multi_consumer_test_template::<MPMCType<(usize, usize), ReallocationPolicy>>(
        1, 16000, 1, 64,
    );
    multi_producer_multi_consumer_test_template::<MPMCType<(usize, usize), ReallocationPolicy>>(
        1, 250, 64, 8,
    );
    multi_producer_multi_consumer_test_template::<MPMCType<(usize, usize), ReallocationPolicy>>(
        1, 2000, 8, 64,
    );
}

#[test]
fn realloc_sc_basic_test() {
    basic_sequential_test_template::<MPSCType<usize, ReallocationPolicy>>(true);
    basic_parallel_test_template::<MPSCType<usize, ReallocationPolicy>>();
    drop_test_template::<MPSCType<DropCounter, ReallocationPolicy>>(true);
}

#[test]
fn realloc_sc_single_producer_single_consumer_test() {
    single_producer_single_consumer_test_template::<MPSCType<(usize, usize), ReallocationPolicy>>(
        1, 10000,
    );
}

#[test]
fn realloc_sc_multi_producer_test() {
    multi_producer_multi_consumer_test_template::<MPSCType<(usize, usize), ReallocationPolicy>>(
        1, 1000, 2, 1,
    );
    multi_producer_multi_consumer_test_template::<MPSCType<(usize, usize), ReallocationPolicy>>(
        1, 250, 128, 1,
    );
}

#[test]
fn realloc_sp_basic_test() {
    basic_sequential_test_template::<SPMCType<usize, ReallocationPolicy>>(true);
    basic_parallel_test_template::<SPMCType<usize, ReallocationPolicy>>();
    drop_test_template::<SPMCType<DropCounter, ReallocationPolicy>>(true);
}

#[test]
fn realloc_sp_single_producer_single_consumer_test() {
    single_producer_single_consumer_test_template::<SPMCType<(usize, usize), ReallocationPolicy>>(
        1, 10000,
    );
}

#[test]
fn realloc_sp_multi_consumer_test() {
    multi_producer_multi_consumer_test_template::<SPMCType<(usize, usize), ReallocationPolicy>>(
        1, 2000, 1, 2,
    );
    multi_producer_multi_consumer_test_template::<SPMCType<(usize, usize), ReallocationPolicy>>(
        1, 32000, 1, 128,
    );
}

#[test]
fn fixed_size_mpmc_basic_test() {
    basic_sequential_test_template::<MPMCType<usize, FixedSizePolicy>>(false);
    basic_parallel_test_template::<MPMCType<usize, FixedSizePolicy>>();
    drop_test_template::<MPMCType<DropCounter, FixedSizePolicy>>(false);
}

#[test]
fn fixed_size_mpmc_single_producer_single_consumer_test() {
    single_producer_single_consumer_test_template::<MPMCType<(usize, usize), FixedSizePolicy>>(
        10, 10000,
    );
}

#[test]
fn fixed_size_mpmc_multi_producer_multi_consumer_test() {
    multi_producer_multi_consumer_test_template::<MPMCType<(usize, usize), FixedSizePolicy>>(
        8, 100, 32, 32,
    );
    multi_producer_multi_consumer_test_template::<MPMCType<(usize, usize), FixedSizePolicy>>(
        64, 1000, 8, 8,
    );
}

#[test]
fn fixed_size_sc_basic_test() {
    basic_sequential_test_template::<MPSCType<usize, FixedSizePolicy>>(false);
    basic_parallel_test_template::<MPSCType<usize, FixedSizePolicy>>();
    drop_test_template::<MPSCType<DropCounter, FixedSizePolicy>>(false);
}

#[test]
fn fixed_size_sc_single_producer_single_consumer_test() {
    single_producer_single_consumer_test_template::<MPSCType<(usize, usize), FixedSizePolicy>>(
        10, 10000,
    );
}

#[test]
fn fixed_size_sc_multi_producer_test() {
    multi_producer_multi_consumer_test_template::<MPSCType<(usize, usize), FixedSizePolicy>>(
        32, 1000, 16, 1,
    );
}

#[test]
fn fixed_size_sp_basic_test() {
    basic_sequential_test_template::<SPMCType<usize, FixedSizePolicy>>(false);
    basic_parallel_test_template::<SPMCType<usize, FixedSizePolicy>>();
    drop_test_template::<SPMCType<DropCounter, FixedSizePolicy>>(false);
}

#[test]
fn fixed_size_sp_single_producer_single_consumer_test() {
    single_producer_single_consumer_test_template::<SPMCType<(usize, usize), FixedSizePolicy>>(
        10, 10000,
    );
}

#[test]
fn fixed_size_sp_multi_consumer_test() {
    multi_producer_multi_consumer_test_template::<SPMCType<(usize, usize), FixedSizePolicy>>(
        32, 16000, 1, 16,
    );
}

// test correctness in sequential context
fn basic_sequential_test_template<QType: QueueType<usize>>(realloc: bool) {
    let (prod, con) = QType::create(2);

    assert_eq!(con.pop(), None);
    prod.append(2);
    prod.append(42);
    assert_some_eq(con.pop(), 2);
    prod.append(33);

    if realloc {
        prod.append(10);
    } else {
        assert!(!prod.append(10), "queue is already full");
    }

    assert_some_eq(con.pop(), 42);
    assert_some_eq(con.pop(), 33);
    if realloc {
        assert_some_eq(con.pop(), 10);
    }
    assert_eq!(con.pop(), None);
}

// primarily tests correct parallel semantics (e.g. Sync & Send)
fn basic_parallel_test_template<QType: QueueType<usize>>() {
    let (prod, con) = QType::create(5);

    let handle = thread::spawn(move || {
        for i in 0..5 {
            prod.append(i);
        }
    });
    handle.join().expect("thread returned unexpected error");

    for i in 0..5 {
        assert_some_eq(con.pop(), i);
    }
}

fn drop_test_template<QType: QueueType<DropCounter>>(realloc: bool) {
    let (prod, con) = QType::create(if realloc { 1 } else { 5 });
    let count = Arc::new(AtomicUsize::new(0));
    {
        let (p, c) = (prod, con);

        for _ in 0..4 {
            p.append(DropCounter::new(count.clone()));
        }
        for _ in 0..3 {
            c.pop().expect("pop failed unexpectedly");
        }
        for _ in 0..2 {
            p.append(DropCounter::new(count.clone()));
        }
        assert_eq!(count.load(Ordering::Relaxed), 3);
    }
    assert_eq!(count.load(Ordering::Relaxed), 6);
}

// use very small size and high throughput to make the test as hard as possible
fn single_producer_single_consumer_test_template<QType: QueueType<(usize, usize)>>(
    size: usize,
    val_count: usize,
) {
    let (prod, con) = QType::create(size);

    let producer = thread::spawn(move || {
        let mut stream = MarkedStream::new(0, val_count);

        for x in &mut stream {
            while !prod.append(x) {
                thread::yield_now();
            }
        }
        return stream;
    });
    let consumer = thread::spawn(move || {
        let mut vec = Vec::new();

        for _count in 0..val_count {
            vec.push(pop_next(&con));
        }

        assert_eq!(con.pop(), None);
        return vec;
    });

    let results = consumer.join().expect("unexpected error in consumer");
    assert_eq!(results.len(), val_count);

    producer
        .join()
        .expect("unexpected error in producer")
        .assert_contained(results.iter());
}

fn multi_producer_multi_consumer_test_template<QType: QueueType<(usize, usize)>>(
    size: usize,
    val_count_per_thread: usize,
    producer_count: usize,
    consumer_count: usize,
) {
    assert!((producer_count * val_count_per_thread) % consumer_count == 0);

    let (prod, con) = QType::create(size);
    let mut producers = Vec::new();
    producers.push(prod);
    for _ in 1..producer_count {
        producers.push(producers[0].clone());
    }
    let mut consumers = Vec::new();
    consumers.push(con);
    for _ in 1..consumer_count {
        consumers.push(consumers[0].clone());
    }

    let mut prod_threads = Vec::new();
    let mut con_threads = Vec::new();

    for i in 0..producer_count {
        let p = producers.pop().unwrap();

        prod_threads.push(thread::spawn(move || {
            let mut stream = MarkedStream::new(i, val_count_per_thread);

            for x in &mut stream {
                while !p.append(x) {
                    thread::yield_now();
                }
            }
            return stream;
        }));
    }

    for _ in 0..consumer_count {
        let c = consumers.pop().unwrap();

        con_threads.push(thread::spawn(move || {
            let mut vec = Vec::new();
            let num = (producer_count * val_count_per_thread) / consumer_count;

            for _count in 0..num {
                vec.push(pop_next(&c));
            }
            return vec;
        }));
    }

    // collect the result of the prod_threads: testers
    let mut testers: Vec<SplitStreamTester> = prod_threads
        .into_iter()
        .map(|prod| {
            prod.join()
                .expect("unexpected error in producer")
                .split_tester()
        })
        .collect();

    // collect the result of the con_threads: vecs containing the elements
    let results = con_threads
        .into_iter()
        .map(|c| c.join().expect("unexpected error in consumer"));

    // apply the testers to the results
    let mut counter = 0;
    for result_vec in results {
        counter += result_vec.len();
        testers
            .iter_mut()
            .for_each(|t| t.assert_partly_contained(result_vec.iter()));
    }

    for t in testers {
        t.assert_complete();
    }
    assert_eq!(counter, producer_count * val_count_per_thread);
}

fn assert_some_eq<T: Eq + std::fmt::Debug>(val: Option<T>, expected: T) {
    assert_eq!(
        val.expect(&format!("expected Some({:?}) instead of None", expected)),
        expected
    );
}

fn pop_next<T>(consumer: &impl MultiConsumer<T>) -> T {
    loop {
        if let Some(x) = consumer.pop() {
            return x;
        }
        thread::yield_now();
    }
}

// helper struct for parallel testing
// generates an ordered stream of index tuples
// and tests whether the generated stream is contained
struct MarkedStream {
    count: usize,
    idx: usize,
    len: usize,
}

impl MarkedStream {
    pub fn new(idx: usize, len: usize) -> MarkedStream {
        MarkedStream { count: 0, idx, len }
    }

    pub fn assert_contained<'a>(&self, iter: Iter<'a, (usize, usize)>) {
        assert_eq!(self.count, self.len);

        let mut counter = 0;
        for val in iter {
            if val.0 == self.idx {
                assert_eq!(val.1, counter);
                counter += 1;
            }
        }

        assert_eq!(counter, self.len);
    }

    pub fn split_tester(&self) -> SplitStreamTester {
        SplitStreamTester::new(self.idx, self.len)
    }
}

impl Iterator for MarkedStream {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<(usize, usize)> {
        if self.count < self.len {
            let res = Some((self.idx, self.count));
            self.count += 1;
            return res;
        } else {
            None
        }
    }
}

// test whether a stream is contained, possibly splitted to many collections
struct SplitStreamTester {
    idx: usize,
    len: usize,
    set: HashSet<usize>,
}

impl SplitStreamTester {
    pub fn new(idx: usize, len: usize) -> SplitStreamTester {
        SplitStreamTester {
            idx,
            len,
            set: HashSet::with_capacity(len),
        }
    }

    pub fn assert_partly_contained<'a>(&mut self, iter: Iter<'a, (usize, usize)>) {
        for val in iter {
            if val.0 == self.idx {
                assert!(!self.set.contains(&val.1));
                self.set.insert(val.1);
            }
        }
    }

    pub fn assert_complete(&self) {
        assert_eq!(self.set.len(), self.len);
    }
}

// counts drops atomically to test for memory safety/leaks
struct DropCounter {
    count: Arc<AtomicUsize>,
}

impl DropCounter {
    fn new(count: Arc<AtomicUsize>) -> DropCounter {
        DropCounter { count }
    }
}

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

// structs for handling different queues uniformly
// illegal clone is handled by panicking
// - is this really safe?
// TODO: different size policies
trait QueueType<T> {
    type PType: MultiProducer<T> + Clone + Send + 'static;
    type CType: MultiConsumer<T> + Clone + Send + 'static;

    fn create(size: usize) -> (Self::PType, Self::CType);
}

trait MultiProducer<T> {
    fn append(&self, value: T) -> bool;
}

trait MultiConsumer<T> {
    fn pop(&self) -> Option<T>;
}

impl<T, P: ProducerConsumerPolicy> MultiProducer<T> for Producer<T, P, FixedSizePolicy> {
    fn append(&self, value: T) -> bool {
        self.append(value)
    }
}

impl<T, P: ProducerConsumerPolicy> MultiProducer<T> for Producer<T, P, ReallocationPolicy> {
    fn append(&self, value: T) -> bool {
        self.append(value);
        true
    }
}

impl<T, P: ProducerConsumerPolicy, S: SizePolicy> MultiConsumer<T> for Consumer<T, P, S> {
    fn pop(&self) -> Option<T> {
        self.pop()
    }
}

struct MPMCType<T, S: SizePolicy> {
    queue: Arc<VecQueue<T, MPMCPolicy, S>>,
}

impl<T, S: SizePolicy> Clone for MPMCType<T, S> {
    fn clone(&self) -> Self {
        MPMCType {
            queue: self.queue.clone(),
        }
    }
}

impl<T> MultiProducer<T> for MPMCType<T, FixedSizePolicy> {
    fn append(&self, value: T) -> bool {
        self.queue.append(value)
    }
}

impl<T> MultiProducer<T> for MPMCType<T, ReallocationPolicy> {
    fn append(&self, value: T) -> bool {
        self.queue.append(value);
        true
    }
}

impl<T, S: SizePolicy> MultiConsumer<T> for MPMCType<T, S> {
    fn pop(&self) -> Option<T> {
        self.queue.pop()
    }
}

impl<T: 'static, S: 'static + SizePolicy> QueueType<T> for MPMCType<T, S>
where
    MPMCType<T, S>: MultiProducer<T>,
{
    type PType = MPMCType<T, S>;
    type CType = MPMCType<T, S>;

    fn create(size: usize) -> (MPMCType<T, S>, MPMCType<T, S>) {
        let queue_w = MPMCType {
            queue: Arc::new(VecQueue::<T, MPMCPolicy, S>::with_capacity(size)),
        };
        (queue_w.clone(), queue_w)
    }
}

struct MPSCType<T, S: SizePolicy> {
    consumer: Consumer<T, MPSCPolicy, S>,
}

impl<T, S: SizePolicy> Clone for MPSCType<T, S> {
    fn clone(&self) -> Self {
        panic!("illegal clone on wrapper for single consumer");
    }
}

impl<T, S: SizePolicy> MultiConsumer<T> for MPSCType<T, S> {
    fn pop(&self) -> Option<T> {
        self.consumer.pop()
    }
}

impl<T: 'static, S: 'static + SizePolicy> QueueType<T> for MPSCType<T, S>
where
    Producer<T, MPSCPolicy, S>: MultiProducer<T>,
{
    type PType = Producer<T, MPSCPolicy, S>;
    type CType = MPSCType<T, S>;

    fn create(size: usize) -> (Self::PType, Self::CType) {
        let (producer, consumer) = VecQueue::<T, MPSCPolicy, S>::with_capacity(size);
        (producer, MPSCType { consumer })
    }
}

struct SPMCType<T, S: SizePolicy> {
    producer: Producer<T, SPMCPolicy, S>,
}

impl<T, S: SizePolicy> Clone for SPMCType<T, S> {
    fn clone(&self) -> Self {
        panic!("illegal clone on wrapper for single producer");
    }
}

impl<T> MultiProducer<T> for SPMCType<T, FixedSizePolicy> {
    fn append(&self, value: T) -> bool {
        self.producer.append(value)
    }
}

impl<T> MultiProducer<T> for SPMCType<T, ReallocationPolicy> {
    fn append(&self, value: T) -> bool {
        self.producer.append(value);
        true
    }
}

impl<T: 'static, S: 'static + SizePolicy> QueueType<T> for SPMCType<T, S>
where
    SPMCType<T, S>: MultiProducer<T>,
{
    type PType = SPMCType<T, S>;
    type CType = Consumer<T, SPMCPolicy, S>;

    fn create(size: usize) -> (Self::PType, Self::CType) {
        let (producer, consumer) = VecQueue::<T, SPMCPolicy, S>::with_capacity(size);
        (SPMCType { producer }, consumer)
    }
}
