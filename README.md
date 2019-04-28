#concurrent-vec-queue

This is an experimental implementation of a concurrent FIFO queue. The algorithm
uses lock-free programming techniques and vec-like memory allocation to achieve
high performance. To enable a correct reallocation, currently some overhead is
required for tracking the size of the queue. Multiple producers and consumers
are supported, and optimized versions for a SPMC or MPSC queue exist.

**Note:** the intention of this crate is to test a specific algorithm, not to
provide a data structure for actual use. The most popular concurrency library
for Rust is [crossbeam](https://github.com/crossbeam-rs/crossbeam), which
contains a concurrent queue with (slightly) better performance.
