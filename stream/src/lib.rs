mod stream;
mod pgsql;
mod kafka;
use crate::stream::StreamProducer;
use std::boxed::Box;
#[macro_use]
extern crate derive_builder;

#[derive(Builder)]
struct Worker<'a> {
    #[builder(default = "\"postgres://postgres@localhost:5432\"")]
    pgurl: &'a str,
    #[builder(default = "\"events\"")]
    table_name: &'a str,
    #[builder(default = "\"payload\"")]
    column_name: &'a str,
    #[builder(default = "\"1=1\"")]
    filter_name: &'a str,
    #[builder(default = "\"events\"")]
    topic_name: &'a str,
    #[builder(default = "100 as usize")]
    buffer_size: usize,
    #[builder(default = "vec![\"localhost:9092\".to_string()]")]
    kafka_brokers: Vec<String>
}

impl <'a> Worker<'a> {
    fn run(&self) {
        let consumer = Box::from(kafka::kafka::KafkaStreamConsumer::new(self.kafka_brokers.clone(), self.topic_name, self.buffer_size));
        let producer = pgsql::pgsql::PostgreSQLStreamProducer::new(self.pgurl, self.table_name, self.column_name, self.filter_name);
        producer.produce(consumer);
    }
}
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
