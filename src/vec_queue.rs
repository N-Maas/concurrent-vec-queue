use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::thread;

// helper trait for handling atomic isize and bool stamps uniformly
pub trait AtomicStamp {
    type Value: Copy;

    fn load(&self, order: Ordering) -> Self::Value;
    fn store(&self, val: Self::Value, order: Ordering);
    fn is_valid(&self, order: Ordering) -> bool;
}

impl AtomicStamp for AtomicIsize {
    type Value = isize;
    fn load(&self, order: Ordering) -> isize {
        self.load(order)
    }
    fn store(&self, val: isize, order: Ordering) {
        self.store(val, order);
    }
    fn is_valid(&self, order: Ordering) -> bool {
        self.load(order) > 0
    }
}

impl AtomicStamp for AtomicBool {
    type Value = bool;
    fn load(&self, order: Ordering) -> bool {
        self.load(order)
    }
    fn store(&self, val: bool, order: Ordering) {
        self.store(val, order);
    }
    fn is_valid(&self, order: Ordering) -> bool {
        self.load(order)
    }
}

// containing value and access stamp
pub struct StampedElement<T, S: AtomicStamp> {
    value: T,
    stamp: S,
}

impl<T, S: AtomicStamp> StampedElement<T, S> {
    pub fn get_stamp(&self) -> S::Value {
        self.stamp.load(Ordering::SeqCst)
    }

    pub fn set_stamp(&mut self, val: S::Value) {
        self.stamp.store(val, Ordering::SeqCst)
    }

    pub fn is_valid(&self) -> bool {
        self.stamp.is_valid(Ordering::SeqCst)
    }

    pub fn read(&mut self) -> T {
        debug_assert!(self.is_valid());

        unsafe { ptr::read(&self.value as *const T) }
    }

    pub fn write(&mut self, val: T) {
        debug_assert!(!self.is_valid());

        unsafe {
            ptr::write(&mut self.value as *mut T, val);
        }
    }
}

// TODO: can peek possibly be safely implemented?
//impl<T: Copy> StampedElement<T> {
//    pub fn peek(&self) -> T {
//        debug_assert!(self.is_valid());
//
//        unsafe { read_volatile::<T>(&self.value as *const T) }
//    }
//}

// fixed size buffer holding elements
// TODO: volatile read/write for capacity/ptr necessary when moving?!
struct Buffer<T> {
    ptr: *mut T,
    capacity: usize,
}

impl<T> Buffer<T> {
    pub fn new(size: usize) -> Buffer<T> {
        let mut v = Vec::<T>::with_capacity(size);

        let buffer = Buffer::<T> {
            ptr: v.as_mut_ptr(),
            capacity: v.capacity(),
        };
        mem::forget(v);

        debug_assert!(buffer.capacity == size);
        return buffer;
    }

    pub fn at(&self, idx: usize) -> &mut T {
        debug_assert!(idx < self.capacity());

        unsafe { &mut *self.ptr.add(idx) }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

// WARNING: does not drop elements
impl<T> Drop for Buffer<T> {
    fn drop(&mut self) {
        unsafe {
            Vec::<T>::from_raw_parts(self.ptr, 0, self.capacity);
        }
    }
}

// TODO: different read/write access to non-MPMC-queues
// the policy set for creating different queue types
// concerning alllowed producer/consumer count, fixed or dynamic size, ...
pub trait ProducerConsumerPolicy {
    type Stamp: AtomicStamp;

    fn new(size: usize) -> Self;

    // increases the given index, performing modulo if necessary
    // returns a pair additionally containing a stamp for the indexed element
    fn increase_idx(
        idx: &AtomicUsize,
        len: usize,
        is_write: bool,
    ) -> (usize, <Self::Stamp as AtomicStamp>::Value);
    // waits for the element to be in valid state for access
    fn wait_for_stamp_on_write<_T>(
        el: &StampedElement<_T, Self::Stamp>,
        len: usize,
        requested: <Self::Stamp as AtomicStamp>::Value,
    );
    fn wait_for_stamp_on_read<_T>(
        el: &StampedElement<_T, Self::Stamp>,
        len: usize,
        requested: <Self::Stamp as AtomicStamp>::Value,
    );

    // request an upper bound, possibly (depending on policy) increasing it
    fn request_len_bound(&self) -> usize;
    fn request_empty_bound(&self) -> usize;

    // hint, that the according value should be decreased
    // might do nothing, depending on the policy
    fn len_decrease_hint(&self);
    fn empty_decrease_hint(&self);

    // initial stamp of elements
    fn init_stamp(idx: usize) -> <Self::Stamp as AtomicStamp>::Value;
}

// test with fixed size
pub struct VecQueue<T, PCPolicy: ProducerConsumerPolicy> {
    //data: Vec<StampedElement<T>>,
    // TODO: not necessarily required to use UnsafeCell (at least with fixed size)?
    data: UnsafeCell<Buffer<StampedElement<T, PCPolicy::Stamp>>>,
    pc_policy: PCPolicy,
    start_idx: AtomicUsize,
    end_idx: AtomicUsize,
    _marker: PhantomData<T>,
}

// TODO: resizing...
impl<T, PCPolicy: ProducerConsumerPolicy> VecQueue<T, PCPolicy> {
    fn at(&self, idx: usize) -> &mut StampedElement<T, PCPolicy::Stamp> {
        unsafe { (*self.data.get()).at(idx) }
    }

    // size must be a power of two
    pub fn new(size: usize) -> VecQueue<T, PCPolicy> {
        // TODO: isize::max_value() + 1 should be legal? (may cause problems with stamp calculations?)
        assert!(size <= isize::max_value() as usize);
        let size = size.next_power_of_two();

        let queue = VecQueue {
            data: UnsafeCell::new(Buffer::new(size)),
            pc_policy: PCPolicy::new(size),
            start_idx: AtomicUsize::new(size),
            end_idx: AtomicUsize::new(size),
            _marker: PhantomData,
        };
        debug_assert!(queue.capacity() == size);

        for idx in 0..size {
            queue.at(idx).set_stamp(PCPolicy::init_stamp(idx));
        }
        return queue;
    }

    pub fn capacity(&self) -> usize {
        unsafe { (*self.data.get()).capacity() }
    }

    // TODO: more relaxed Ordering possible?
    pub fn append(&self, value: T) -> bool {
        let len = self.pc_policy.request_len_bound();

        // test whether queue is full
        if len >= self.capacity() {
            self.pc_policy.len_decrease_hint();
            return false;
        }

        let (idx, stamp) = PCPolicy::increase_idx(&self.end_idx, self.capacity(), true);
        PCPolicy::wait_for_stamp_on_write(self.at(idx), self.capacity(), stamp);
        self.at(idx).write(value);
        self.at(idx).set_stamp(stamp);

        self.pc_policy.empty_decrease_hint();
        return true;
    }

    pub fn pop(&self) -> Option<T> {
        let empty = self.pc_policy.request_empty_bound();

        // test whether queue is empty
        if empty >= self.capacity() {
            self.pc_policy.empty_decrease_hint();
            return None;
        }

        let (idx, stamp) = PCPolicy::increase_idx(&self.start_idx, self.capacity(), false);
        PCPolicy::wait_for_stamp_on_read(self.at(idx), self.capacity(), stamp);
        let val = self.at(idx).read();
        self.at(idx).set_stamp(stamp);

        self.pc_policy.len_decrease_hint();
        return Some(val);
    }
}

impl<T, P: ProducerConsumerPolicy> Drop for VecQueue<T, P> {
    fn drop(&mut self) {
        if self.pc_policy.request_len_bound() == 0 {
            return;
        }

        let start = self.start_idx.load(Ordering::Relaxed);
        let max = self.end_idx.load(Ordering::Relaxed);

        // drop all remaining values
        for idx in start..max {
            self.at(idx & (self.capacity() - 1)).read();
        }
    }
}

unsafe impl<T, P: ProducerConsumerPolicy> Sync for VecQueue<T, P> {}
unsafe impl<T, P: ProducerConsumerPolicy> Send for VecQueue<T, P> {}

// multiple producer/multiple consumer policy
pub struct MPMCPolicy {
    empty: AtomicUsize,
    len: AtomicUsize,
}

impl ProducerConsumerPolicy for MPMCPolicy {
    type Stamp = AtomicIsize;

    fn new(size: usize) -> MPMCPolicy {
        MPMCPolicy {
            empty: AtomicUsize::new(size),
            len: AtomicUsize::new(0),
        }
    }

    fn increase_idx(idx: &AtomicUsize, len: usize, is_write: bool) -> (usize, isize) {
        debug_assert!(len.is_power_of_two());
        // wrapped overflow in AtomicUsize should function correctly (because len is power of two)
        let stamp_sign = if is_write { 1 } else { -1 };
        let old_val = idx.fetch_add(1, Ordering::SeqCst);

        return (
            old_val & (len - 1),
            stamp_sign * ((old_val as isize) & isize::max_value()),
        );
    }

    // waits for the element to be in valid state for access
    fn wait_for_stamp_on_write<_T>(
        el: &StampedElement<_T, AtomicIsize>,
        len: usize,
        requested: isize,
    ) {
        debug_assert!(len.is_power_of_two());
        debug_assert!(len <= isize::max_value() as usize);

        while !(el.get_stamp() == (len as isize) - requested) {
            thread::yield_now();
        }
    }

    fn wait_for_stamp_on_read<_T>(
        el: &StampedElement<_T, AtomicIsize>,
        _len: usize,
        requested: isize,
    ) {
        while !(el.get_stamp() == -requested) {
            thread::yield_now();
        }
    }

    // request an upper bound, possibly (depending on policy) increasing it
    fn request_len_bound(&self) -> usize {
        self.len.fetch_add(1, Ordering::SeqCst)
    }

    fn request_empty_bound(&self) -> usize {
        self.empty.fetch_add(1, Ordering::SeqCst)
    }

    fn len_decrease_hint(&self) {
        self.len.fetch_sub(1, Ordering::SeqCst);
    }

    fn empty_decrease_hint(&self) {
        self.empty.fetch_sub(1, Ordering::SeqCst);
    }

    fn init_stamp(idx: usize) -> isize {
        debug_assert!(idx <= isize::max_value() as usize);

        -(idx as isize)
    }
}
