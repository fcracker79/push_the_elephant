pub mod stream {
    extern crate kafka;
    use std::error;
    use kafka::producer::{Producer, Record, RequiredAcks};
    // use kafka::error::Error as KafkaError;
    use std::time::Duration;
    use crate::common;
    use log::{info, debug};
    /// A stream consumer for Kafka
    pub struct KafkaStreamConsumer<'a> {
        /// The buffer of messages that will be sent in batch
        buffer: Vec<common::SourceElement>,
        /// The max buffer size
        max_buffer_size: usize,
        /// The topic where messages are sent
        topic: &'a str,
        /// Kafka messages producer
        producer: Producer
    }

    impl <'a> KafkaStreamConsumer<'a> {
        /// Returns a new Stream Consumer for Kafka
        ///
        /// # Arguments
        ///
        /// * brokers - the list of Kafka hosts
        /// * topic - the topic where message are sent
        /// * max_buffer_size - the max number of messages that are sent in batch
        ///
        pub fn new(brokers: Vec<String>, topic: &'a str, max_buffer_size: usize) -> Result<KafkaStreamConsumer, Box<error::Error>> {
            info!(target: "kafka", "Connecting to brokers {:?}, topic {:?}, max_buffer_size {:?}", brokers, topic, max_buffer_size);
            Ok(KafkaStreamConsumer{
                buffer: Vec::new(),
                max_buffer_size,
                topic,
                producer: 
                    Producer::from_hosts(brokers)
                        .with_ack_timeout(Duration::from_secs(1))
                        .with_required_acks(RequiredAcks::One)
                        .create()?
            })
        }
    }

    impl <'a> common::StreamConsumer for KafkaStreamConsumer<'a> {
        fn write(& mut self, element: common::SourceElement) -> Result<(), Box<error::Error>> {
            info!(target: "kafka", "Writing element");
            debug!(target: "kafka", "Writing element {:?}", element);
            self.buffer.push(element);
            if self.max_buffer_size <= self.buffer.len() {
                return self.flush();
            }
            return Ok(());
        }

        fn flush(&mut self) -> Result<(), Box<error::Error>> {
            info!("Flushing Kafka buffer, size: {:?}", self.buffer.len());
            self.producer.send_all(
                &self.buffer.iter().map(
                    |x| Record {topic: self.topic, partition: -1, key: &*x.id, value: &*x.data}
                ).collect::<Vec<Record<'_, &str, &[u8]>>>())?;
            self.buffer.clear();
            return Ok(());
        }
    }
}
