use concurrent_vec_queue::*;
use std::ops::Deref;
use std::sync::Arc;
use std::thread;

#[macro_use]
extern crate criterion;

use criterion::Criterion;

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
        b.iter(|| {
            alternate_push_pop(
                VecQueue::<usize, MPMCPolicy, ReallocationPolicy>::with_capacity(1),
                1000,
            )
        })
    });
    c.bench_function("alternate push & pop fixed size 1000", |b| {
        b.iter(|| {
            alternate_push_pop(
                VecQueue::<usize, MPMCPolicy, FixedSizePolicy>::with_capacity(1),
                1000,
            )
        })
    });
    c.bench_function("peak growth realloc 1000", |b| {
        b.iter(|| {
            peak_growth(
                VecQueue::<usize, MPMCPolicy, ReallocationPolicy>::with_capacity(1),
                1000,
            )
        })
    });
    c.bench_function("fluctuating growth realloc 1000", |b| {
        b.iter(|| {
            fluctuating_growth(
                VecQueue::<usize, MPMCPolicy, ReallocationPolicy>::with_capacity(1),
                10,
                90,
                10,
            )
        })
    });
}

fn parallel_benches(c: &mut Criterion) {
    // threading and initialization overhead is estimated 20% - 40% of running time
    c.bench_function("parallel push & pop fixed size 8000", |b| {
        b.iter(|| parallel_push_pop_fs(8, 1000))
    });
    c.bench_function("parallel growth realloc 8000", |b| {
        b.iter(|| parallel_growth_realloc(8, 10, 90, 10))
    });
}

fn expensive_benches(c: &mut Criterion) {
    c.bench_function("highly parallel growth 128000", |b| {
        b.iter(|| parallel_growth_realloc(64, 20, 90, 10))
    });
}

criterion_group!(
    benches,
    sequential_benches,
    vec_benches,
    parallel_benches,
    expensive_benches
);
criterion_main!(benches);

// queue trait for benches
trait BQueue {
    fn push(&mut self, val: usize);
    fn pop(&mut self) -> usize;
}

impl BQueue for Vec<usize> {
    fn push(&mut self, val: usize) {
        self.push(val);
    }
    fn pop(&mut self) -> usize {
        self.pop().unwrap()
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

fn parallel_push_pop_fs(thread_cnt: usize, el_cnt: usize) -> usize {
    let q_ptr = Arc::new(VecQueue::<usize, MPMCPolicy, FixedSizePolicy>::with_capacity(thread_cnt));
    let mut threads = Vec::new();
    let mut sum = 0;

    for _ in 0..thread_cnt {
        let cloned_ptr = q_ptr.clone();

        threads.push(thread::spawn(move || {
            alternate_push_pop(cloned_ptr, el_cnt)
        }));
    }

    for t in threads.into_iter() {
        sum += t.join().unwrap()
    }
    sum
}

fn parallel_growth_realloc(
    thread_cnt: usize,
    peak_cnt: usize,
    peak_height: usize,
    peak_growth: usize,
) -> usize {
    let q_ptr = Arc::new(VecQueue::<usize, MPMCPolicy, ReallocationPolicy>::with_capacity(1));
    let mut threads = Vec::new();
    let mut sum = 0;

    for _ in 0..thread_cnt {
        let cloned_ptr = q_ptr.clone();

        threads.push(thread::spawn(move || {
            fluctuating_growth(cloned_ptr, peak_cnt, peak_height, peak_growth)
        }));
    }

    for t in threads.into_iter() {
        sum += t.join().unwrap()
    }
    sum
}
