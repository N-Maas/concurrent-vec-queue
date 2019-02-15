use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::atomic::AtomicIsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;

// containing value and access stamp
pub struct VFElement<T> {
    value: T,
    stamp: AtomicIsize,
}

impl<T> VFElement<T> {
    pub fn get_stamp(&self) -> isize {
        self.stamp.load(Ordering::SeqCst)
    }

    pub fn set_stamp(&mut self, val: isize) {
        self.stamp.store(val, Ordering::SeqCst)
    }

    pub fn is_valid(&self) -> bool {
        self.get_stamp() > 0
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
//impl<T: Copy> VFElement<T> {
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

// test with fixed size
pub struct VecQueue<T> {
    //data: Vec<VFElement<T>>,
    // TODO: not necessarily required to use UnsafeCell (at least with fixed size)?
    data: UnsafeCell<Buffer<VFElement<T>>>,
    start_idx: AtomicUsize,
    end_idx: AtomicUsize,
    min_len: AtomicUsize,
    max_len: AtomicUsize,
    _marker: PhantomData<T>,
}

// TODO: resizing...
impl<T> VecQueue<T> {
    fn at(&self, idx: usize) -> &mut VFElement<T> {
        unsafe { (*self.data.get()).at(idx) }
    }

    // increases the index, performing modulo if necessary
    // returning a couple of the previous value with and without modulo len (the latter as isize stamp)
    fn increase_idx(idx: &AtomicUsize, len: usize) -> (usize, isize) {
        debug_assert!(len.is_power_of_two());
        debug_assert!(len <= isize::max_value() as usize);
        // wrapped overflow in AtomicUsize should function correctly (because len is power of two)
        let old_val = idx.fetch_add(1, Ordering::SeqCst);
        return (old_val & (len - 1), (old_val as isize) & isize::max_value());
    }

    // size must be a power of two
    pub fn new(size: usize) -> VecQueue<T> {
        assert!(size <= isize::max_value() as usize);
        let size = size.next_power_of_two();

        let queue = VecQueue {
            data: UnsafeCell::new(Buffer::new(size)),
            start_idx: AtomicUsize::new(size),
            end_idx: AtomicUsize::new(size),
            min_len: AtomicUsize::new(0),
            max_len: AtomicUsize::new(0),
            _marker: PhantomData,
        };
        debug_assert!(queue.capacity() == size);

        for idx in 0..size {
            queue.at(idx).set_stamp(-(idx as isize));
        }
        return queue;
    }

    pub fn capacity(&self) -> usize {
        unsafe { (*self.data.get()).capacity() }
    }

    // TODO: more relaxed Ordering possible?
    pub fn append(&self, value: T) -> bool {
        let len = self.max_len.fetch_add(1, Ordering::SeqCst);

        // test whether queue is full
        if len >= self.capacity() {
            self.max_len.fetch_sub(1, Ordering::SeqCst);
            return false;
        }

        let (idx, stamp) = Self::increase_idx(&self.end_idx, self.capacity());

        while !(self.at(idx).get_stamp() == (self.capacity() as isize) - stamp) {
            thread::yield_now();
        }
        self.at(idx).write(value);
        self.at(idx).set_stamp(stamp);

        self.min_len.fetch_add(1, Ordering::SeqCst);
        return true;
    }

    pub fn pop(&self) -> Option<T> {
        let len = self.min_len.fetch_sub(1, Ordering::SeqCst);

        // test whether queue is empty
        if len <= 0 {
            self.min_len.fetch_add(1, Ordering::SeqCst);
            return None;
        }

        let (idx, stamp) = Self::increase_idx(&self.start_idx, self.capacity());

        while !(self.at(idx).get_stamp() == stamp) {
            thread::yield_now();
        }
        let val = self.at(idx).read();
        self.at(idx).set_stamp(-stamp);

        self.max_len.fetch_sub(1, Ordering::SeqCst);
        return Some(val);
    }
}

impl<T> Drop for VecQueue<T> {
    fn drop(&mut self) {
        if self.max_len.load(Ordering::Relaxed) == 0 {
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

unsafe impl<T> Sync for VecQueue<T> {}
unsafe impl<T> Send for VecQueue<T> {}
