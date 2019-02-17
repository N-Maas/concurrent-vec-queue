use concurrent_vec_queue::ProducerConsumerPolicy;
use concurrent_vec_queue::MPMCPolicy;
use concurrent_vec_queue::VecQueue;

use std::collections::HashSet;
use std::slice::Iter;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

#[test]
// test correctness in sequential context
fn basic_sequential_test() {
    let queue = VecQueue::<usize, MPMCPolicy>::new(2);

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

#[test]
// primarily tests correct parallel semantics (e.g. Sync & Send)
fn basic_parallel_test() {
    let queue = Arc::new(VecQueue::<usize, MPMCPolicy>::new(5));
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

#[test]
// use very small size and high throughput to make the test as hard as possible
fn single_producer_single_consumer_test() {
    let size = 10;
    let val_count = 10000;

    let queue = Arc::new(VecQueue::<(usize, usize), MPMCPolicy>::new(size));
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

#[test]
fn drop_test() {
    let count = Arc::new(AtomicUsize::new(0));
    {
        let queue = VecQueue::<DropCounter, MPMCPolicy>::new(5);

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

#[test]
fn multi_producer_multi_consumer_test() {
    let size = 8;
    let val_count_per_thread = 1000;
    let thread_count = 16;

    let queue = Arc::new(VecQueue::<(usize, usize), MPMCPolicy>::new(size));
    let mut producers = Vec::new();
    let mut consumers = Vec::new();

    for i in 0..thread_count {
        let queue_ptr = queue.clone();

        producers.push(thread::spawn(move || {
            let mut stream = MarkedStream::new(i, val_count_per_thread);

            for x in &mut stream {
                while !queue_ptr.append(x) {}
            }
            return stream;
        }));
    }

    for _ in 0..thread_count {
        let queue_ptr = queue.clone();

        consumers.push(thread::spawn(move || {
            let mut vec = Vec::new();

            for _count in 0..val_count_per_thread {
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
    assert_eq!(counter, thread_count * val_count_per_thread);
}

fn assert_some_eq<T: Eq + std::fmt::Display + std::fmt::Debug>(val: Option<T>, expected: T) {
    assert_eq!(val.expect("expected Some({}) instead of None"), expected);
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
