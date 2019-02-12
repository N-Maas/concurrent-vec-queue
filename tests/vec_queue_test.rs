use concurrent_vec_queue::VecQueue;
use std::slice::Iter;
use std::sync::Arc;
use std::thread;

#[test]
// test correctness in sequential context
fn basic_sequential_test() {
    let queue = VecQueue::new(2);

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
    let queue = Arc::new(VecQueue::new(5));
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
    let val_count = 100000;

    let queue = Arc::new(VecQueue::new(size));
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
            loop {
                if let Some(x) = consumer_ptr.pop() {
                    vec.push(x);
                    break;
                }
            }
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

fn assert_some_eq<T: Eq + std::fmt::Display + std::fmt::Debug>(val: Option<T>, expected: T) {
    assert_eq!(val.expect("expected Some({}) instead of None"), expected);
}

// helper struct for parallel testing
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

                if counter == self.len {
                    break;
                }
            }
        }

        assert_eq!(counter, self.len);
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
