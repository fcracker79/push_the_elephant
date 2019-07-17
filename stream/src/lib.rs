mod stream;
mod pgsql;
mod kafka;
use crate::stream::StreamProducer;
use std::time::Duration;

#[macro_use]
extern crate derive_builder;

#[derive(Builder)]
pub struct Worker<'a> {
    #[builder(default = "\"postgres://postgres@localhost:5433\"")]
    pgurl: &'a str,
    #[builder(default = "\"events\"")]
    table_name: &'a str,
    #[builder(default = "\"payload\"")]
    column_name: &'a str,
    #[builder(default = "\"events.activity\"")]
    channel: &'a str,
    #[builder(default = "\"events\"")]
    topic_name: &'a str,
    #[builder(default = "100 as usize")]
    buffer_size: usize,
    #[builder(default = "vec![\"localhost:9092\".to_string()]")]
    kafka_brokers: Vec<String>,
    #[builder(default = "Duration::from_secs(3)")]
    notify_timeout: Duration,
    #[builder(default = "Duration::from_secs(60)")]
    notify_timeout_total: Duration
}

impl <'a> Worker<'a> {
    pub fn run(&self) {
        let mut consumer = kafka::stream::KafkaStreamConsumer::new(self.kafka_brokers.clone(), self.topic_name, self.buffer_size);
        let producer = pgsql::stream::PostgreSQLListenStreamProducer::new(self.pgurl, self.table_name, self.column_name, self.channel, self.notify_timeout_total, self.notify_timeout);
        producer.produce(&mut consumer);
    }
}
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
