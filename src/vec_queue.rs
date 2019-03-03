use std::boxed::Box;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicPtr, AtomicUsize, Ordering};
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

// the policy set for creating different queue types
// concerning alllowed producer/consumer count, fixed or dynamic size, ...

// policy for handling stamps, depending on producer/consumer count
pub trait ProducerConsumerPolicy {
    type Stamp: AtomicStamp;

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
}

// policy for handling exceeded capacity
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
    size_policy: SPolicy,
    empty: AtomicUsize,
    len: AtomicUsize,
    start_idx: AtomicUsize,
    end_idx: AtomicUsize,
    _marker: PhantomData<T>,
}

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
    // specifically, a size of zero is not possible, but is increased to 1
    fn new(size: usize) -> Self {
        // TODO: isize::max_value() + 1 should be legal? (may cause problems with stamp calculations?)
        assert!(size <= isize::max_value() as usize);
        let size = size.next_power_of_two();

        let queue = VecQueue {
            data: UnsafeCell::new(Buffer::new(size)),
            size_policy: SPolicy::new(size),
            empty: AtomicUsize::new(size),
            len: AtomicUsize::new(0),
            start_idx: AtomicUsize::new(size),
            end_idx: AtomicUsize::new(size),
            _marker: PhantomData,
        };
        debug_assert!(queue.capacity() == size);

        for idx in 0..size {
            // TODO: atomic operation probably unnecessary?
            queue.at(idx).set_stamp(PCPolicy::to_stamp(idx, false));
        }
        return queue;
    }

    pub fn capacity(&self) -> usize {
        unsafe { (*self.data.get()).capacity() }
    }

    // TODO: more relaxed Ordering possible?
    pub fn append(&self, value: T) -> SPolicy::AppendReturnType {
        let len = self.len.fetch_add(1, Ordering::SeqCst);

        // test whether queue is full
        // TODO: loop necessary? TODO: Seems to fix deadlock???
        if len >= self.capacity() {
            match self.size_policy.capacity_exceeded(self) {
                Some(x) => {
                    self.len.fetch_sub(1, Ordering::SeqCst);
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

        self.empty.fetch_sub(1, Ordering::SeqCst);
        return self.size_policy.return_success();
    }

    pub fn pop(&self) -> Option<T> {
        let empty = self.empty.fetch_add(1, Ordering::SeqCst);

        // test whether queue is empty
        if empty >= self.capacity() {
            self.empty.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        self.size_policy.invoce_barrier(self);

        let (idx, stamp) = Self::increase_idx(&self.start_idx, self.capacity(), false);

        PCPolicy::wait_for_stamp_on_read(self.at(idx), self.capacity(), stamp);
        let val = self.at(idx).read();
        self.at(idx).set_stamp(stamp);

        self.len.fetch_sub(1, Ordering::SeqCst);
        return Some(val);
    }
}

// TODO: rework with drop_in_place?
impl<T, P: ProducerConsumerPolicy, S: SizePolicy> Drop for VecQueue<T, P, S> {
    fn drop(&mut self) {
        if !mem::needs_drop::<T>() || self.len.load(Ordering::SeqCst) == 0 {
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
pub struct MPMCPolicy {}

impl ProducerConsumerPolicy for MPMCPolicy {
    type Stamp = AtomicIsize;

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

        println!(
            "WRITE LOOP, stamp: {:?}, expected: {:?}, {:?}",
            el.get_stamp(),
            (cap as isize) - requested,
            thread::current().id()
        );
        while !(el.get_stamp() == (cap as isize) - requested) {
            thread::yield_now();
        }
        println!(">>>WRITE SUCCESS, {:?}", thread::current().id());
    }

    fn wait_for_stamp_on_read<_T>(
        el: &StampedElement<_T, AtomicIsize>,
        _cap: usize,
        requested: isize,
    ) {
        println!(
            "READ LOOP, stamp: {:?}, expected: {:?}, {:?}",
            el.get_stamp(),
            -requested,
            thread::current().id()
        );
        while !(el.get_stamp() == -requested) {
            thread::yield_now();
        }
        println!(">>>READ SUCCESS, {:?}", thread::current().id());
    }
}

// multiple producer/single consumer policy
pub struct MPSCPolicy {}

impl ProducerConsumerPolicy for MPSCPolicy {
    type Stamp = AtomicBool;

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
}

// single producer/multiple consumer policy
pub struct SPMCPolicy {}

impl ProducerConsumerPolicy for SPMCPolicy {
    type Stamp = AtomicBool;

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

// reallocation policy
const COPY_BLOCK_SIZE: usize = 255;
const THREAD_COUNT_MASK: usize = isize::max_value() as usize;
const LOCK_MASK: usize = THREAD_COUNT_MASK + 1;
// const COPY_COUNT_LOCKED: usize = usize::max_value();

fn is_locked(l_and_count: usize) -> bool {
    (l_and_count & LOCK_MASK) != 0
}

fn to_thread_count(l_and_count: usize) -> usize {
    l_and_count & THREAD_COUNT_MASK
}

// temporarily for reallocation needed (heap allocated) data
pub struct ReallocationData<T, S: AtomicStamp> {
    new_buffer: Buffer<StampedElement<T, S>>,
    start_flag: AtomicBool,
    copy_idx: AtomicUsize,
}

pub struct ReallocationPolicy {
    // synchronizes initial access and completion of all copy tasks
    lock_and_t_count: AtomicUsize,
    // synchronizes the copying
    copy_t_count: AtomicUsize,
    // must be casted to correct type
    ra_ptr: AtomicPtr<()>,
}

impl ReallocationPolicy {
    // help copying the data, but only if the thread_count could be increased successfully
    fn try_join<_T, _P: ProducerConsumerPolicy>(
        &self,
        queue: &VecQueue<_T, _P, Self>,
    ) -> Option<()> {
        let l_tc = self.lock_and_t_count.fetch_add(1, Ordering::SeqCst);
        // if not locked anymore, return
        if !is_locked(l_tc) {
            self.lock_and_t_count.fetch_sub(1, Ordering::SeqCst);
            return None;
        }

        let mut result = Err(1);
        loop {
            // println!("WAIT TRY JOIN");
            match result {
                Ok(_) => {
                    return self.join_copying(queue);
                }
                Err(count) => {
                    // if thread count is 0, the work is nearly done already
                    if count == 0 {
                        // TODO: this could happen at init, too
                        // - better wait for increased count?
                        println!("*** COUNT ZERO: {:?} ***", thread::current().id());
                        return self.await_completion();
                    }
                    // else: try increase the thread count
                    result = self.copy_t_count.compare_exchange(
                        count,
                        count + 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                }
            }
        }
    }

    // help copying the data, as soon as a stable state is reached
    fn join_copying<T, P: ProducerConsumerPolicy>(
        &self,
        queue: &VecQueue<T, P, Self>,
    ) -> Option<()> {
        debug_assert!(is_locked(self.lock_and_t_count.load(Ordering::SeqCst)));
        debug_assert!(self.copy_t_count.load(Ordering::SeqCst) > 0);
        debug_assert!(self.ra_ptr.load(Ordering::SeqCst) != ptr::null_mut());

        // copy_t_count > 0 asserts self.ra_ptr is initialized
        let realloc_data: &ReallocationData<T, P::Stamp> =
            unsafe { &*(self.ra_ptr.load(Ordering::SeqCst) as *mut ReallocationData<T, P::Stamp>) };
        // first, wait for ra_ptr to be initialized
        //{
        //    loop {
        //        // println!("WAIT FOR PTR");
        //        let p = self.ra_ptr.load(Ordering::SeqCst);
        //        if p != ptr::null_mut() {
        //            break unsafe { &*(p as *mut ReallocationData<T, P::Stamp>) };
        //        }
        //        thread::yield_now();
        //    }
        //};

        // wait for start_flag, indicating a stable state
        let len = queue.len.load(Ordering::SeqCst);
        let empty = queue.empty.load(Ordering::SeqCst);
        let bias = len + empty - queue.capacity();
        let t_count = to_thread_count(self.lock_and_t_count.load(Ordering::SeqCst));
        let copy_cnt = self.copy_t_count.load(Ordering::SeqCst);
            println!(
               "len: {:?}, empty: {:?}, bias: {:?}, t_count: {:?}, copy_count: {:?}",
               len,
               empty,
               bias,
               t_count,
               copy_cnt
            );
        println!("+(1) WAIT FOR START: {:?} - {:?}", thread::current().id(), realloc_data.start_flag.load(Ordering::SeqCst));
        while !realloc_data.start_flag.load(Ordering::SeqCst) {
            debug_assert!(self.ra_ptr.load(Ordering::SeqCst) != ptr::null_mut());
            debug_assert!(is_locked(self.lock_and_t_count.load(Ordering::SeqCst)));

            // thread count can't decrease in this phase, so this results in a pessimistic estimate
            let t_count = to_thread_count(self.lock_and_t_count.load(Ordering::SeqCst));
            let bias = queue.len.load(Ordering::SeqCst) + queue.empty.load(Ordering::SeqCst)
                - queue.capacity();
            debug_assert!(bias >= t_count);

            // if bias == thread count, no thread is behind the barrier anymore and a stable state is reached
            if bias == t_count {
                realloc_data.start_flag.store(true, Ordering::SeqCst);
                break;
            }
            //println!(
            //    "len: {:?}, empty: {:?}, bias: {:?}, t_count: {:?}",
            //    queue.len.load(Ordering::SeqCst),
            //    queue.empty.load(Ordering::SeqCst),
            //    bias,
            //    t_count
            //);
            thread::yield_now();
        }
        println!("-(1) SUCCESS: {:?}", thread::current().id());

        Self::copy_data(realloc_data, queue);

        // if this is the last arriving thread, invoke finalize (for this thread only!)
        if to_thread_count(self.copy_t_count.fetch_sub(1, Ordering::SeqCst)) == 1 {
            return self.finalize(queue);
        }
        self.await_completion()
    }

    // must be called only by one thread per reallocation, performs the final steps
    fn copy_data<T, P: ProducerConsumerPolicy>(
        realloc: &ReallocationData<T, P::Stamp>,
        queue: &VecQueue<T, P, Self>,
    ) {
        let block_size = (COPY_BLOCK_SIZE / mem::size_of::<T>()) + 1;
        let start_idx = queue.start_idx.load(Ordering::SeqCst);
        let end_idx = queue.end_idx.load(Ordering::SeqCst);
        let new_cap = realloc.new_buffer.capacity();
        let old_cap = queue.capacity();

        loop {
            let start = realloc.copy_idx.fetch_add(block_size, Ordering::SeqCst);
            if start >= new_cap {
                break;
            }

            //println!("LOOP START");
            //println!(
            //    "old_cap: {:?}, new_cap: {:?}, block_size: {:?}, start: {:?}",
            //    old_cap, new_cap, block_size, start
            //);
            for i in start..usize::min(start + block_size, new_cap) {
                let q_idx = start_idx + i;
                //println!("i: {:?}, q_idx: {:?}", i, q_idx);
                let el = &mut realloc.new_buffer.at(i);
                // TODO: remove this line
                el.set_stamp(P::to_stamp(q_idx, false));
                if q_idx < end_idx {
                    el.write(queue.at(q_idx & (old_cap - 1)).read());
                    el.set_stamp(P::to_stamp(i + new_cap, true));
                } else {
                    el.set_stamp(P::to_stamp(i, false));
                }
            }
            //println!("LOOP END");
        }

        debug_assert!(start_idx == queue.start_idx.load(Ordering::SeqCst));
        debug_assert!(end_idx == queue.end_idx.load(Ordering::SeqCst));
    }

    // must be called only by one thread per reallocation, performs the final steps
    fn finalize<T, P: ProducerConsumerPolicy>(&self, queue: &VecQueue<T, P, Self>) -> Option<()> {
        println!("*** FINALIZE: {:?} ***", thread::current().id());
        debug_assert!(is_locked(self.lock_and_t_count.load(Ordering::SeqCst)));
        debug_assert!(self.copy_t_count.load(Ordering::SeqCst) == 0);

        let realloc_ptr = self.ra_ptr.load(Ordering::SeqCst) as *mut ReallocationData<T, P::Stamp>;
        // exchange buffers and drop the ReallocationData
        unsafe {
            ptr::swap(queue.data.get(), &mut (*realloc_ptr).new_buffer);
            ptr::drop_in_place(realloc_ptr);
        }
        self.ra_ptr.store(ptr::null_mut(), Ordering::SeqCst);

        // reset indizes (start_idx is reset to capacity)
        // and increase empty count by capacity / 2
        let diff = queue.end_idx.load(Ordering::SeqCst) - queue.start_idx.load(Ordering::SeqCst);
        let cap = queue.capacity();
        queue.start_idx.store(cap, Ordering::SeqCst);
        queue.end_idx.store(cap + diff, Ordering::SeqCst);
        queue.empty.fetch_add(cap >> 1, Ordering::SeqCst);

        debug_assert!((diff == 0) || queue.at(diff - 1).is_valid());
        debug_assert!(!queue.at(diff).is_valid());

        // unlock, now business as usual can continue
        self.lock_and_t_count.fetch_sub(1, Ordering::SeqCst);
        self.lock_and_t_count
            .fetch_and(THREAD_COUNT_MASK, Ordering::SeqCst);
        println!("EXTENDED TO {:?}", cap);
        None
    }

    // waits until the lock is released
    fn await_completion(&self) -> Option<()> {
        println!("+(2) AWAIT COMPLETION: {:?}", thread::current().id());
        while is_locked(self.lock_and_t_count.load(Ordering::SeqCst)) {
            thread::yield_now();
        }

        debug_assert!(to_thread_count(self.lock_and_t_count.load(Ordering::SeqCst)) > 0);
        self.lock_and_t_count.fetch_sub(1, Ordering::SeqCst);
        println!("-(2) COMPLETED: {:?}", thread::current().id());
        None
    }
}

impl SizePolicy for ReallocationPolicy {
    type AppendReturnType = ();

    fn new(_size: usize) -> Self {
        ReallocationPolicy {
            lock_and_t_count: AtomicUsize::new(0),
            copy_t_count: AtomicUsize::new(0),
            ra_ptr: AtomicPtr::new(ptr::null_mut()),
        }
    }

    fn return_success(&self) {
        ()
    }

    // try init the lock
    fn capacity_exceeded<T, P: ProducerConsumerPolicy>(
        &self,
        queue: &VecQueue<T, P, Self>,
    ) -> Option<()> {
        loop {
            // println!("EXCEED LOOP: {:?}", thread::current().id());
            // try set lock to true and thread_count to 1
            // if lock != 0, it is still locked or the last reallocation is not completed yet
            match self.lock_and_t_count.compare_exchange(
                0,
                LOCK_MASK + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    // init the reallocation data
                    debug_assert!(self.copy_t_count.load(Ordering::SeqCst) == 0);
                    debug_assert!(self.ra_ptr.load(Ordering::SeqCst) == ptr::null_mut());

                    let init_data = Box::new(ReallocationData::<T, P::Stamp> {
                        new_buffer: Buffer::new(2 * queue.capacity()),
                        start_flag: AtomicBool::new(false),
                        copy_idx: AtomicUsize::new(0),
                    });
                    debug_assert!(init_data.new_buffer.capacity().is_power_of_two());

                    self.ra_ptr
                        .store(Box::into_raw(init_data) as *mut (), Ordering::SeqCst);
                    // TODO: correct?
                    self.copy_t_count.store(1, Ordering::SeqCst);
                    println!("INIT JOIN: {:?}", thread::current().id());
                    return self.join_copying(queue);
                }

                Err(val) => {
                    // if already locked, join the copying
                    if is_locked(val) {
                        return self.try_join(queue);
                    }
                    // else: retry setting the lock
                    thread::yield_now();
                }
            }
        }
    }

    fn invoce_barrier<_T, _P: ProducerConsumerPolicy>(&self, queue: &VecQueue<_T, _P, Self>) {
        let lock = self.lock_and_t_count.load(Ordering::SeqCst);
        if is_locked(lock) {
            println!("SEE LOCK: {:?}", thread::current().id());
            self.try_join(queue);
        }
    }
}
