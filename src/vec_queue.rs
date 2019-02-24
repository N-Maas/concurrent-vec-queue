use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
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
    pub fn new(size: usize) -> Self {
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

    // returns the fitting stamp for the index (without modulo)
    fn to_stamp(idx: usize, is_write: bool) -> <Self::Stamp as AtomicStamp>::Value;

    // waits for the element to be in valid state for access
    fn wait_for_stamp_on_write<_T>(
        el: &StampedElement<_T, Self::Stamp>,
        cap: usize,
        requested: <Self::Stamp as AtomicStamp>::Value,
    );
    fn wait_for_stamp_on_read<_T>(
        el: &StampedElement<_T, Self::Stamp>,
        cap: usize,
        requested: <Self::Stamp as AtomicStamp>::Value,
    );

    // request an upper bound, possibly (depending on policy) increasing it
    fn request_len_bound(&self, cap: usize) -> usize;
    fn request_empty_bound(&self, cap: usize) -> usize;

    // hint, that the according value should be decreased
    // might do nothing, depending on the policy
    fn len_decrease_hint(&self);
    fn empty_decrease_hint(&self);
}

// the policy set for handling exceeded capacity
pub trait SizePolicy: Sized {
    type AppendReturnType;

    fn new(size: usize) -> Self;

    // returns true or (), usually
    fn return_success(&self) -> Self::AppendReturnType;

    // returns None if append should continue,
    // Some(x) if append should immediately return x
    // if returning, the respective counter is decreased, too
    fn capacity_exceeded<_T, _P: ProducerConsumerPolicy>(
        &self,
        queue: &VecQueue<_T, _P, Self>,
    ) -> Option<Self::AppendReturnType>;

    // called after size test to enable e.g. reallocation
    fn invoce_barrier<_T, _P: ProducerConsumerPolicy>(&self, queue: &VecQueue<_T, _P, Self>);
}

// test with fixed size
pub struct VecQueue<T, PCPolicy: ProducerConsumerPolicy, SPolicy: SizePolicy> {
    //data: Vec<StampedElement<T>>,
    // TODO: not necessarily required to use UnsafeCell (at least with fixed size)?
    data: UnsafeCell<Buffer<StampedElement<T, PCPolicy::Stamp>>>,
    pc_policy: PCPolicy,
    size_policy: SPolicy,
    start_idx: AtomicUsize,
    end_idx: AtomicUsize,
    _marker: PhantomData<T>,
}

// TODO: resizing...
impl<T, PCPolicy: ProducerConsumerPolicy, SPolicy: SizePolicy> VecQueue<T, PCPolicy, SPolicy> {
    fn at(&self, idx: usize) -> &mut StampedElement<T, PCPolicy::Stamp> {
        unsafe { (*self.data.get()).at(idx) }
    }

    // increases the given index, returning the old value modulo capacity
    // additionally returns the stamp for the index
    fn increase_idx(
        counter: &AtomicUsize,
        len: usize,
        is_write: bool,
    ) -> (usize, <PCPolicy::Stamp as AtomicStamp>::Value) {
        debug_assert!(len.is_power_of_two());

        let idx = counter.fetch_add(1, Ordering::SeqCst);
        (idx & (len - 1), PCPolicy::to_stamp(idx, is_write))
    }

    // size is always a power of two
    fn new(size: usize) -> Self {
        // TODO: isize::max_value() + 1 should be legal? (may cause problems with stamp calculations?)
        assert!(size <= isize::max_value() as usize);
        let size = size.next_power_of_two();

        let queue = VecQueue {
            data: UnsafeCell::new(Buffer::new(size)),
            pc_policy: PCPolicy::new(size),
            size_policy: SPolicy::new(size),
            start_idx: AtomicUsize::new(size),
            end_idx: AtomicUsize::new(size),
            _marker: PhantomData,
        };
        debug_assert!(queue.capacity() == size);

        for idx in 0..size {
            queue.at(idx).set_stamp(PCPolicy::to_stamp(idx, false));
        }
        return queue;
    }

    pub fn capacity(&self) -> usize {
        unsafe { (*self.data.get()).capacity() }
    }

    // TODO: more relaxed Ordering possible?
    pub fn append(&self, value: T) -> SPolicy::AppendReturnType {
        let len = self.pc_policy.request_len_bound(self.capacity());

        // test whether queue is full
        if len >= self.capacity() {
            match self.size_policy.capacity_exceeded(self) {
                Some(x) => {
                    self.pc_policy.len_decrease_hint();
                    return x;
                }
                None => {}
            }
        }
        self.size_policy.invoce_barrier(self);

        let (idx, stamp) = Self::increase_idx(&self.end_idx, self.capacity(), true);

        PCPolicy::wait_for_stamp_on_write(self.at(idx), self.capacity(), stamp);
        self.at(idx).write(value);
        self.at(idx).set_stamp(stamp);

        self.pc_policy.empty_decrease_hint();
        return self.size_policy.return_success();
    }

    pub fn pop(&self) -> Option<T> {
        let empty = self.pc_policy.request_empty_bound(self.capacity());

        // test whether queue is empty
        if empty >= self.capacity() {
            self.pc_policy.empty_decrease_hint();
            return None;
        }
        self.size_policy.invoce_barrier(self);

        let (idx, stamp) = Self::increase_idx(&self.start_idx, self.capacity(), false);

        PCPolicy::wait_for_stamp_on_read(self.at(idx), self.capacity(), stamp);
        let val = self.at(idx).read();
        self.at(idx).set_stamp(stamp);

        self.pc_policy.len_decrease_hint();
        return Some(val);
    }
}

// TODO: rework with drop_in_place?
impl<T, P: ProducerConsumerPolicy, S: SizePolicy> Drop for VecQueue<T, P, S> {
    fn drop(&mut self) {
        if !mem::needs_drop::<T>() || (self.pc_policy.request_len_bound(self.capacity())) == 0 {
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

unsafe impl<T, P: ProducerConsumerPolicy, S: SizePolicy> Sync for VecQueue<T, P, S> {}
unsafe impl<T, P: ProducerConsumerPolicy, S: SizePolicy> Send for VecQueue<T, P, S> {}

// ---
// implement the (PC-policy dependent) possibilities for creating a queue
// semantics are based on the std::sync::mpsc::channel semantics for Sender/Receiver

pub struct Producer<T, PCPolicy: ProducerConsumerPolicy, SPolicy: SizePolicy> {
    ptr: Arc<VecQueue<T, PCPolicy, SPolicy>>,
    _not_sync: PhantomData<*const ()>,
}

impl<T, P: ProducerConsumerPolicy, S: SizePolicy> Producer<T, P, S> {
    pub fn append(&self, value: T) -> S::AppendReturnType {
        self.ptr.append(value)
    }
}

//impl<T, P: ProducerConsumerPolicy> !Sync for Producer<T, P> {}
unsafe impl<T, P: ProducerConsumerPolicy, S: SizePolicy> Send for Producer<T, P, S> {}

impl<T, S: SizePolicy> Clone for Producer<T, MPSCPolicy, S> {
    fn clone(&self) -> Self {
        Producer {
            ptr: self.ptr.clone(),
            _not_sync: PhantomData,
        }
    }
}

pub struct Consumer<T, PCPolicy: ProducerConsumerPolicy, SPolicy: SizePolicy> {
    ptr: Arc<VecQueue<T, PCPolicy, SPolicy>>,
    _not_sync: PhantomData<*const ()>,
}

impl<T, P: ProducerConsumerPolicy, S: SizePolicy> Consumer<T, P, S> {
    pub fn pop(&self) -> Option<T> {
        self.ptr.pop()
    }
}

//impl<T, P: ProducerConsumerPolicy> !Sync for Consumer<T, P> {}
unsafe impl<T, P: ProducerConsumerPolicy, S: SizePolicy> Send for Consumer<T, P, S> {}

impl<T, S: SizePolicy> Clone for Consumer<T, SPMCPolicy, S> {
    fn clone(&self) -> Self {
        Consumer {
            ptr: self.ptr.clone(),
            _not_sync: PhantomData,
        }
    }
}

impl<T, S: SizePolicy> VecQueue<T, MPMCPolicy, S> {
    pub fn with_capacity(size: usize) -> VecQueue<T, MPMCPolicy, S> {
        VecQueue::new(size)
    }
}

impl<T, S: SizePolicy> VecQueue<T, MPSCPolicy, S> {
    pub fn with_capacity(size: usize) -> (Producer<T, MPSCPolicy, S>, Consumer<T, MPSCPolicy, S>) {
        let ptr = Arc::new(VecQueue::<T, MPSCPolicy, S>::new(size));
        (
            Producer {
                ptr: ptr.clone(),
                _not_sync: PhantomData,
            },
            Consumer {
                ptr,
                _not_sync: PhantomData,
            },
        )
    }
}

impl<T, S: SizePolicy> VecQueue<T, SPMCPolicy, S> {
    pub fn with_capacity(size: usize) -> (Producer<T, SPMCPolicy, S>, Consumer<T, SPMCPolicy, S>) {
        let ptr = Arc::new(VecQueue::<T, SPMCPolicy, S>::new(size));
        (
            Producer {
                ptr: ptr.clone(),
                _not_sync: PhantomData,
            },
            Consumer {
                ptr,
                _not_sync: PhantomData,
            },
        )
    }
}

// ---
// implement the policies

// multiple producer/multiple consumer policy
pub struct MPMCPolicy {
    empty: AtomicUsize,
    len: AtomicUsize,
}

impl ProducerConsumerPolicy for MPMCPolicy {
    type Stamp = AtomicIsize;

    fn new(size: usize) -> Self {
        MPMCPolicy {
            empty: AtomicUsize::new(size),
            len: AtomicUsize::new(0),
        }
    }

    fn to_stamp(idx: usize, is_write: bool) -> isize {
        // wrapped overflow should function correctly here
        (if is_write { 1 } else { -1 }) * ((idx as isize) & isize::max_value())
    }

    // waits for the correct access stamp
    fn wait_for_stamp_on_write<_T>(
        el: &StampedElement<_T, AtomicIsize>,
        cap: usize,
        requested: isize,
    ) {
        debug_assert!(cap.is_power_of_two());
        debug_assert!(cap <= isize::max_value() as usize);

        while !(el.get_stamp() == (cap as isize) - requested) {
            thread::yield_now();
        }
    }

    fn wait_for_stamp_on_read<_T>(
        el: &StampedElement<_T, AtomicIsize>,
        _cap: usize,
        requested: isize,
    ) {
        while !(el.get_stamp() == -requested) {
            thread::yield_now();
        }
    }

    // request an upper bound, increasing it
    fn request_len_bound(&self, _cap: usize) -> usize {
        self.len.fetch_add(1, Ordering::SeqCst)
    }

    fn request_empty_bound(&self, _cap: usize) -> usize {
        self.empty.fetch_add(1, Ordering::SeqCst)
    }

    fn len_decrease_hint(&self) {
        self.len.fetch_sub(1, Ordering::SeqCst);
    }

    fn empty_decrease_hint(&self) {
        self.empty.fetch_sub(1, Ordering::SeqCst);
    }
}

// multiple producer/single consumer policy
pub struct MPSCPolicy {
    len: AtomicUsize,
}

impl ProducerConsumerPolicy for MPSCPolicy {
    type Stamp = AtomicBool;

    fn new(_size: usize) -> Self {
        MPSCPolicy {
            len: AtomicUsize::new(0),
        }
    }

    fn to_stamp(_idx: usize, is_write: bool) -> bool {
        is_write
    }

    // write does not need to wait
    fn wait_for_stamp_on_write<_T>(
        _el: &StampedElement<_T, AtomicBool>,
        _len: usize,
        requested: bool,
    ) {
        debug_assert!(requested);
    }

    fn wait_for_stamp_on_read<_T>(
        el: &StampedElement<_T, AtomicBool>,
        _len: usize,
        requested: bool,
    ) {
        debug_assert!(!requested);

        while !el.get_stamp() {
            thread::yield_now();
        }
    }

    fn request_len_bound(&self, _cap: usize) -> usize {
        self.len.fetch_add(1, Ordering::SeqCst)
    }

    // with only one consumer, empty can be calculated using len
    // len might temporarily be higher then the capacity
    fn request_empty_bound(&self, cap: usize) -> usize {
        let len = self.len.load(Ordering::SeqCst);

        if len < cap {
            cap - len
        } else {
            0
        }
    }

    fn len_decrease_hint(&self) {
        self.len.fetch_sub(1, Ordering::SeqCst);
    }

    fn empty_decrease_hint(&self) {}
}

// single producer/multiple consumer policy
pub struct SPMCPolicy {
    empty: AtomicUsize,
}

impl ProducerConsumerPolicy for SPMCPolicy {
    type Stamp = AtomicBool;

    fn new(size: usize) -> Self {
        SPMCPolicy {
            empty: AtomicUsize::new(size),
        }
    }

    fn to_stamp(_idx: usize, is_write: bool) -> bool {
        is_write
    }

    // write does not need to wait
    fn wait_for_stamp_on_write<_T>(
        el: &StampedElement<_T, AtomicBool>,
        _len: usize,
        requested: bool,
    ) {
        debug_assert!(requested);

        while el.get_stamp() {
            thread::yield_now();
        }
    }

    fn wait_for_stamp_on_read<_T>(
        _el: &StampedElement<_T, AtomicBool>,
        _len: usize,
        requested: bool,
    ) {
        debug_assert!(!requested);
    }

    // with only one producer, len can be calculated using empty
    // empty might temporarily be higher then the capacity
    fn request_len_bound(&self, cap: usize) -> usize {
        let empty = self.empty.load(Ordering::SeqCst);

        if empty < cap {
            cap - empty
        } else {
            0
        }
    }

    fn request_empty_bound(&self, _cap: usize) -> usize {
        self.empty.fetch_add(1, Ordering::SeqCst)
    }

    fn len_decrease_hint(&self) {}

    fn empty_decrease_hint(&self) {
        self.empty.fetch_sub(1, Ordering::SeqCst);
    }
}

// fixed size policy
// does effectively nothing (other then cancelling append)
pub struct FixedSizePolicy {}

impl SizePolicy for FixedSizePolicy {
    type AppendReturnType = bool;

    fn new(_size: usize) -> Self {
        FixedSizePolicy {}
    }

    fn return_success(&self) -> bool {
        true
    }

    fn capacity_exceeded<_T, _P: ProducerConsumerPolicy>(
        &self,
        _queue: &VecQueue<_T, _P, Self>,
    ) -> Option<bool> {
        Some(false)
    }

    fn invoce_barrier<_T, _P: ProducerConsumerPolicy>(&self, _queue: &VecQueue<_T, _P, Self>) {}
}

// TODO: ReallocationPolicy
