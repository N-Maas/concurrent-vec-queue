use concurrent_vec_queue::MPMCPolicy;
use concurrent_vec_queue::MPSCPolicy;
use concurrent_vec_queue::ProducerConsumerPolicy;
use concurrent_vec_queue::SPMCPolicy;
use concurrent_vec_queue::VecQueue;

use std::collections::HashSet;
use std::slice::Iter;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

#[test]
fn mpmc_basic_test() {
    basic_sequential_test_template::<MPMCPolicy>();
    basic_parallel_test_template::<MPMCPolicy>();
    drop_test_template::<MPMCPolicy>();
}

#[test]
fn mpmc_single_producer_single_consumer_test() {
    single_producer_single_consumer_test_template::<MPMCPolicy>(10, 10000);
}

#[test]
fn mpmc_multi_producer_multi_consumer_test() {
    multi_producer_multi_consumer_test_template::<MPMCPolicy>(64, 1000, 16, 16);
}

#[test]
fn sc_basic_test() {
    basic_sequential_test_template::<MPSCPolicy>();
    basic_parallel_test_template::<MPSCPolicy>();
    drop_test_template::<MPSCPolicy>();
}

#[test]
fn sc_single_producer_single_consumer_test() {
    single_producer_single_consumer_test_template::<MPSCPolicy>(10, 10000);
}

#[test]
fn sc_multi_producer_test() {
    multi_producer_multi_consumer_test_template::<MPSCPolicy>(32, 1000, 16, 1);
}

#[test]
fn sp_basic_test() {
    basic_sequential_test_template::<SPMCPolicy>();
    basic_parallel_test_template::<SPMCPolicy>();
    drop_test_template::<SPMCPolicy>();
}

#[test]
fn sp_single_producer_single_consumer_test() {
    single_producer_single_consumer_test_template::<SPMCPolicy>(10, 10000);
}

#[test]
fn sp_multi_consumer_test() {
    multi_producer_multi_consumer_test_template::<SPMCPolicy>(32, 16000, 1, 16);
}

// test correctness in sequential context
fn basic_sequential_test_template<PCPolicy: ProducerConsumerPolicy>() {
    let queue = VecQueue::<usize, PCPolicy>::new(2);

    queue.append(2);
    queue.append(42);
    assert_some_eq(queue.pop(), 2);

    queue.append(33);
    // only for fixed size
    assert!(!queue.append(10), "queue is already full");

    assert_some_eq(queue.pop(), 42);
    assert_some_eq(queue.pop(), 33);
    assert_eq!(queue.pop(), None);
}

// primarily tests correct parallel semantics (e.g. Sync & Send)
fn basic_parallel_test_template<PCPolicy: 'static + ProducerConsumerPolicy>() {
    let queue = Arc::new(VecQueue::<usize, PCPolicy>::new(5));
    let queue_ptr = queue.clone();

    let handle = thread::spawn(move || {
        for i in 0..5 {
            queue_ptr.append(i);
        }
    });
    handle.join().expect("thread returned unexpected error");

    for i in 0..5 {
        assert_some_eq(queue.pop(), i);
    }
}

fn drop_test_template<PCPolicy: ProducerConsumerPolicy>() {
    let count = Arc::new(AtomicUsize::new(0));
    {
        let queue = VecQueue::<DropCounter, PCPolicy>::new(5);

        for _ in 0..4 {
            queue.append(DropCounter::new(count.clone()));
        }
        for _ in 0..3 {
            queue.pop().expect("pop failed unexpectedly");
        }
        for _ in 0..2 {
            queue.append(DropCounter::new(count.clone()));
        }
        assert_eq!(count.load(Ordering::Relaxed), 3);
    }
    assert_eq!(count.load(Ordering::Relaxed), 6);
}

// use very small size and high throughput to make the test as hard as possible
fn single_producer_single_consumer_test_template<PCPolicy: 'static + ProducerConsumerPolicy>(
    size: usize,
    val_count: usize,
) {
    let queue = Arc::new(VecQueue::<(usize, usize), PCPolicy>::new(size));
    let producer_ptr = queue.clone();
    let consumer_ptr = queue.clone();

    let producer = thread::spawn(move || {
        let mut stream = MarkedStream::new(0, val_count);

        for x in &mut stream {
            while !producer_ptr.append(x) {}
        }
        return stream;
    });
    let consumer = thread::spawn(move || {
        let mut vec = Vec::new();

        for _count in 0..val_count {
            vec.push(pop_next(&*consumer_ptr));
        }

        assert_eq!(consumer_ptr.pop(), None);
        return vec;
    });

    let results = consumer.join().expect("unexpected error in consumer");
    assert_eq!(results.len(), val_count);

    producer
        .join()
        .expect("unexpected error in producer")
        .assert_contained(results.iter());
}

// TODO: read/write access
fn multi_producer_multi_consumer_test_template<PCPolicy: 'static + ProducerConsumerPolicy>(
    size: usize,
    val_count_per_thread: usize,
    producer_count: usize,
    consumer_count: usize,
) {
    assert!((producer_count * val_count_per_thread) % consumer_count == 0);

    let queue = Arc::new(VecQueue::<(usize, usize), PCPolicy>::new(size));
    let mut producers = Vec::new();
    let mut consumers = Vec::new();

    for i in 0..producer_count {
        let queue_ptr = queue.clone();

        producers.push(thread::spawn(move || {
            let mut stream = MarkedStream::new(i, val_count_per_thread);

            for x in &mut stream {
                while !queue_ptr.append(x) {}
            }
            return stream;
        }));
    }

    for _ in 0..consumer_count {
        let queue_ptr = queue.clone();

        consumers.push(thread::spawn(move || {
            let mut vec = Vec::new();
            let num = (producer_count * val_count_per_thread) / consumer_count;

            for _count in 0..num {
                vec.push(pop_next(&*queue_ptr));
            }
            return vec;
        }));
    }

    // collect the result of the producers: testers
    let mut testers: Vec<SplitStreamTester> = producers
        .into_iter()
        .map(|prod| {
            prod.join()
                .expect("unexpected error in producer")
                .split_tester()
        })
        .collect();

    // collect the result of the consumers: vecs containing the elements
    let results = consumers
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

fn assert_some_eq<T: Eq + std::fmt::Display + std::fmt::Debug>(val: Option<T>, expected: T) {
    assert_eq!(
        val.expect(&format!("expected Some({}) instead of None", expected)),
        expected
    );
}

fn pop_next<T, S: ProducerConsumerPolicy>(queue: &VecQueue<T, S>) -> T {
    loop {
        if let Some(x) = queue.pop() {
            return x;
        }
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
