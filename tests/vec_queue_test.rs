use concurrent_vec_queue::VecQueue;

#[test]
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

fn assert_some_eq<T: Eq + std::fmt::Display + std::fmt::Debug>(val: Option<T>, expected: T) {
	match val {
		Some(x) => assert_eq!(x, expected),
		None => assert!(false, "expected Some({}) instead of None", expected)
	}
}
