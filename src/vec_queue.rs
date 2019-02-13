use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem;
use std::ptr::read_volatile;
use std::ptr::write_volatile;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;

// volatile and containing a valid flag
// ensures volatile access, but no (!) synchronisation
pub struct VFElement<T> {
    value: T,
    // TODO: needs to be atomic?
    is_valid: bool,
}

impl<T> VFElement<T> {
    pub fn is_valid(&self) -> bool {
        unsafe { read_volatile::<bool>(&self.is_valid as *const bool) }
    }

    pub fn set_valid(&mut self, val: bool) {
        unsafe {
            write_volatile::<bool>(&mut self.is_valid as *mut bool, val);
        }
    }

    pub fn read(&mut self) -> T {
        debug_assert!(self.is_valid());

        let val = unsafe { read_volatile::<T>(&self.value as *const T) };
        self.set_valid(false);
        val
    }

    pub fn write(&mut self, val: T) {
        debug_assert!(!self.is_valid());

        unsafe {
            write_volatile::<T>(&mut self.value as *mut T, val);
        }
        self.set_valid(true);
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

    // increases the index, performing modulo if necessary, returning the previous value
    fn increase_idx(idx: &AtomicUsize, len: usize) -> usize {
        let old_val = idx.fetch_add(1, Ordering::SeqCst);

        if old_val >= len {
            let mod_val = old_val % len;
            idx.compare_and_swap(old_val + 1, mod_val + 1, Ordering::SeqCst);
            return mod_val;
        }
        return old_val;
    }

    pub fn capacity(&self) -> usize {
        unsafe { (*self.data.get()).capacity() }
    }

    pub fn new(size: usize) -> VecQueue<T> {
        let queue = VecQueue {
            data: UnsafeCell::new(Buffer::new(size)),
            start_idx: AtomicUsize::new(0),
            end_idx: AtomicUsize::new(0),
            min_len: AtomicUsize::new(0),
            max_len: AtomicUsize::new(0),
            _marker: PhantomData,
        };
        debug_assert!(queue.capacity() == size);

        for idx in 0..size {
            queue.at(idx).set_valid(false);
        }
        return queue;
    }

    // TODO: more relaxed Ordering possible?
    pub fn append(&self, value: T) -> bool {
        let len = self.max_len.fetch_add(1, Ordering::SeqCst);

        // test whether queue is full
        if len >= self.capacity() {
            self.max_len.fetch_sub(1, Ordering::SeqCst);
            return false;
        }

        self.min_len.fetch_add(1, Ordering::SeqCst);
        let idx = Self::increase_idx(&self.end_idx, self.capacity());
        self.at(idx).write(value);
        return true;
    }

    pub fn pop(&self) -> Option<T> {
        let len = self.min_len.fetch_sub(1, Ordering::SeqCst);

        // test whether queue is empty
        if len <= 0 {
            self.min_len.fetch_add(1, Ordering::SeqCst);
            return None;
        }

        let idx = Self::increase_idx(&self.start_idx, self.capacity());
        while !self.at(idx).is_valid() {
            thread::yield_now();
        }
        let val = self.at(idx).read();
        self.max_len.fetch_sub(1, Ordering::SeqCst);
        return Some(val);
    }
}

impl<T> Drop for VecQueue<T> {
    fn drop(&mut self) {
        if self.max_len.load(Ordering::Relaxed) == 0 {
            return;
        }

        let len = self.capacity();
        let max = self.end_idx.load(Ordering::Relaxed) % len;
        let mut idx = self.start_idx.load(Ordering::Relaxed);

        // drop all remaining values
        loop {
            self.at(idx).read();

            idx += 1;
            if idx >= len {
                idx %= len;
            }
            if idx == max {
                break;
            }
        }
    }
}

unsafe impl<T> Sync for VecQueue<T> {}
unsafe impl<T> Send for VecQueue<T> {}
