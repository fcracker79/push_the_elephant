pub mod kafka {
    extern crate kafka;
    use kafka::producer::{Producer, Record, RequiredAcks};
    // use kafka::error::Error as KafkaError;
    use std::time::Duration;
    use crate::stream;
    
    pub struct KafkaStreamConsumer<'a> {
        buffer: Vec<stream::SourceElement>,
        max_buffer_size: usize,
        topic: &'a str,
        producer: Producer
    }

    impl <'a> KafkaStreamConsumer<'a> {
        pub fn new(brokers: Vec<String>, topic: &str, max_buffer_size: usize) -> KafkaStreamConsumer {
            KafkaStreamConsumer{
                buffer: Vec::new(),
                max_buffer_size: max_buffer_size,
                topic: topic,
                producer: 
                    Producer::from_hosts(brokers)
                        .with_ack_timeout(Duration::from_secs(1))
                        .with_required_acks(RequiredAcks::One)
                        .create().unwrap()
            }
        }
    }

    impl <'a> stream::StreamConsumer for KafkaStreamConsumer<'a> {
        fn write(& mut self, element: stream::SourceElement) {
            self.buffer.push(element);
            if self.max_buffer_size <= self.buffer.len() {
                self.flush();
            }
        }

        fn flush(&mut self) {
            self.producer.send_all(
                &self.buffer.iter().map(
                    |x| Record {topic: self.topic, partition: -1, key: &*x.id, value: &*x.data}
                ).collect::<Vec<Record<'_, &str, &[u8]>>>()).unwrap();
            self.buffer.clear();
        }
    }
}
