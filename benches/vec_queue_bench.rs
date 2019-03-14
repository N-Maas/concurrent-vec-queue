use concurrent_vec_queue::*;
use criterion::Criterion;
use crossbeam_queue::ArrayQueue;
use crossbeam_queue::SegQueue;
use std::ops::Deref;
use std::ops::Fn;
use std::sync::Arc;
use std::thread;

#[macro_use]
extern crate criterion;
extern crate crossbeam_queue;

fn vec_benches(c: &mut Criterion) {
    c.bench_function("alternate push & pop vec 1000", |b| {
        b.iter(|| alternate_push_pop(Vec::with_capacity(1), 1000))
    });
    c.bench_function("peak growth vec 1000", |b| {
        b.iter(|| peak_growth(Vec::with_capacity(1), 1000))
    });
    c.bench_function("fluctuating growth vec 1000", |b| {
        b.iter(|| fluctuating_growth(Vec::with_capacity(1), 10, 90, 10))
    });
}

fn sequential_benches(c: &mut Criterion) {
    c.bench_function("alternate push & pop realloc 1000", |b| {
        b.iter(|| alternate_push_pop(new_re_vec_queue(), 1000))
    });
    c.bench_function("alternate push & pop fixed size 1000", |b| {
        b.iter(|| alternate_push_pop(new_fs_vec_queue(1), 1000))
    });
    c.bench_function("peak growth realloc 1000", |b| {
        b.iter(|| peak_growth(new_re_vec_queue(), 1000))
    });
    c.bench_function("fluctuating growth realloc 1000", |b| {
        b.iter(|| fluctuating_growth(new_re_vec_queue(), 10, 90, 10))
    });
}

fn parallel_benches(c: &mut Criterion) {
    c.bench_function("parallel push & pop fixed size 8000", |b| {
        b.iter(|| parallel_push_pop(Arc::new(new_fs_vec_queue(8)), 8, 1000))
    });
    c.bench_function("parallel growth realloc 8000", |b| {
        b.iter(|| parallel_growth(Arc::new(new_re_vec_queue()), 8, 10, 90, 10))
    });
    // threading and initialization overhead is estimated 70% of running time
    // (comparison to fluctutating growth benchmark)
    c.bench_function("parallel growth reference 1000", |b| {
        b.iter(|| parallel_growth(Arc::new(new_re_vec_queue()), 1, 10, 90, 10))
    });
}

fn expensive_benches(c: &mut Criterion) {
    c.bench_function("highly parallel growth 128000", |b| {
        b.iter(|| parallel_growth(Arc::new(new_re_vec_queue()), 64, 20, 90, 10))
    });
    c.bench_function("parallel growth 8 threads 128000", |b| {
        b.iter(|| parallel_growth(Arc::new(new_re_vec_queue()), 8, 80, 180, 20))
    });
}

fn crossbeam_seq_benches(c: &mut Criterion) {
    c.bench_function("alternate push & pop CB::SegQueue 1000", |b| {
        b.iter(|| alternate_push_pop(SegQueue::<usize>::new(), 1000))
    });
    c.bench_function("alternate push & pop CB::ArrayQueue 1000", |b| {
        b.iter(|| alternate_push_pop(ArrayQueue::<usize>::new(1), 1000))
    });
    c.bench_function("peak growth CB::SegQueue 1000", |b| {
        b.iter(|| peak_growth(SegQueue::<usize>::new(), 1000))
    });
    c.bench_function("fluctuating growth CB::SegQueue 1000", |b| {
        b.iter(|| fluctuating_growth(SegQueue::<usize>::new(), 10, 90, 10))
    });
}

fn crossbeam_parallel_benches(c: &mut Criterion) {
    c.bench_function("parallel push & pop CB::ArrayQueue 8000", |b| {
        b.iter(|| parallel_push_pop(Arc::new(ArrayQueue::<usize>::new(8)), 8, 1000))
    });
    c.bench_function("parallel growth CB::SegQueue 8000", |b| {
        b.iter(|| parallel_growth(Arc::new(SegQueue::<usize>::new()), 8, 10, 90, 10))
    });
    c.bench_function("parallel growth reference CB::SegQueue 1000", |b| {
        b.iter(|| parallel_growth(Arc::new(SegQueue::<usize>::new()), 1, 10, 90, 10))
    });
}

fn crossbeam_expensive_benches(c: &mut Criterion) {
    c.bench_function("highly parallel growth CB::SegQueue 128000", |b| {
        b.iter(|| parallel_growth(Arc::new(SegQueue::<usize>::new()), 64, 20, 90, 10))
    });
    c.bench_function("parallel growth 8 threads CB::SegQueue 128000", |b| {
        b.iter(|| parallel_growth(Arc::new(SegQueue::<usize>::new()), 8, 80, 180, 20))
    });
}

criterion_group!(
    benches,
    sequential_benches,
    vec_benches,
    parallel_benches,
    expensive_benches,
    crossbeam_seq_benches,
    crossbeam_parallel_benches,
    crossbeam_expensive_benches,
);
criterion_main!(benches);

fn new_fs_vec_queue(size: usize) -> VecQueue<usize, MPMCPolicy, FixedSizePolicy> {
    VecQueue::<usize, MPMCPolicy, FixedSizePolicy>::with_capacity(size)
}

fn new_re_vec_queue() -> VecQueue<usize, MPMCPolicy, ReallocationPolicy> {
    VecQueue::<usize, MPMCPolicy, ReallocationPolicy>::with_capacity(1)
}

// queue trait for benches
trait BQueue {
    fn push(&mut self, val: usize);
    fn pop(&mut self) -> usize;
}

impl BQueue for Vec<usize> {
    fn push(&mut self, val: usize) {
        Vec::<usize>::push(self, val);
    }
    fn pop(&mut self) -> usize {
        Vec::<usize>::pop(self).unwrap()
    }
}

impl<S: SizePolicy> BQueue for VecQueue<usize, MPMCPolicy, S>
where
    S::AppendReturnType: Eq,
{
    fn push(&mut self, val: usize) {
        assert!(self.append(val) == S::return_success());
    }
    fn pop(&mut self) -> usize {
        VecQueue::<usize, MPMCPolicy, S>::pop(self).unwrap()
    }
}

impl<S: SizePolicy> BQueue for Arc<VecQueue<usize, MPMCPolicy, S>>
where
    S::AppendReturnType: Eq,
{
    fn push(&mut self, val: usize) {
        assert!(self.deref().append(val) == S::return_success());
    }
    fn pop(&mut self) -> usize {
        VecQueue::<usize, MPMCPolicy, S>::pop(self.deref()).unwrap()
    }
}

impl BQueue for ArrayQueue<usize> {
    fn push(&mut self, val: usize) {
        ArrayQueue::<usize>::push(self, val).unwrap();
    }
    fn pop(&mut self) -> usize {
        ArrayQueue::<usize>::pop(self).unwrap()
    }
}

impl BQueue for Arc<ArrayQueue<usize>> {
    fn push(&mut self, val: usize) {
        ArrayQueue::<usize>::push(self.deref(), val).unwrap();
    }
    fn pop(&mut self) -> usize {
        ArrayQueue::<usize>::pop(self.deref()).unwrap()
    }
}

impl BQueue for SegQueue<usize> {
    fn push(&mut self, val: usize) {
        SegQueue::<usize>::push(self, val);
    }
    fn pop(&mut self) -> usize {
        SegQueue::<usize>::pop(self).unwrap()
    }
}

impl BQueue for Arc<SegQueue<usize>> {
    fn push(&mut self, val: usize) {
        SegQueue::<usize>::push(self.deref(), val);
    }
    fn pop(&mut self) -> usize {
        SegQueue::<usize>::pop(self.deref()).unwrap()
    }
}

fn alternate_push_pop(mut queue: impl BQueue, count: usize) -> usize {
    let mut sum = 0;

    for i in 0..count {
        queue.push(i);
        sum += queue.pop();
    }
    sum
}

fn peak_growth(mut queue: impl BQueue, count: usize) -> usize {
    let mut sum = 0;

    for i in 0..count {
        queue.push(i);
    }
    for _ in 0..count {
        sum += queue.pop();
    }
    sum
}

fn fluctuating_growth(
    mut queue: impl BQueue,
    peak_cnt: usize,
    peak_height: usize,
    peak_growth: usize,
) -> usize {
    let mut sum = 0;

    for i in 0..peak_cnt {
        for j in 0..(peak_height + peak_growth) {
            queue.push(i + j);
        }
        for _ in 0..peak_height {
            sum += queue.pop();
        }
    }
    for _ in 0..(peak_cnt * peak_growth) {
        sum += queue.pop();
    }
    sum
}

fn parallel_push_pop(
    q_ptr: impl BQueue + Clone + Send + 'static,
    thread_cnt: usize,
    el_cnt: usize,
) -> usize {
    apply_parallel(q_ptr, thread_cnt, move |ptr| {
        alternate_push_pop(ptr, el_cnt)
    })
}

fn parallel_growth(
    q_ptr: impl BQueue + Clone + Send + 'static,
    thread_cnt: usize,
    peak_cnt: usize,
    peak_height: usize,
    peak_growth: usize,
) -> usize {
    apply_parallel(q_ptr, thread_cnt, move |ptr| {
        fluctuating_growth(ptr, peak_cnt, peak_height, peak_growth)
    })
}

fn apply_parallel<Q: BQueue + Clone + Send + 'static>(
    q_ptr: Q,
    thread_cnt: usize,
    func: impl (Fn(Q) -> usize) + Copy + Send + 'static,
) -> usize {
    let mut threads = Vec::new();
    let mut sum = 0;

    for _ in 0..thread_cnt {
        let cloned_ptr = q_ptr.clone();

        threads.push(thread::spawn(move || func(cloned_ptr)));
    }

    for t in threads.into_iter() {
        sum += t.join().unwrap()
    }
    sum
}
