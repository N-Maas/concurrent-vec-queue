
mod vec_queue;

pub use self::vec_queue::ProducerConsumerPolicy;
pub use self::vec_queue::MPMCPolicy;
pub use self::vec_queue::MPSCPolicy;
pub use self::vec_queue::SPMCPolicy;
pub use self::vec_queue::MultiProducer;
pub use self::vec_queue::SingleProducer;
pub use self::vec_queue::MultiConsumer;
pub use self::vec_queue::SingleConsumer;
pub use self::vec_queue::Producer;
pub use self::vec_queue::Consumer;
pub use self::vec_queue::VecQueue;
