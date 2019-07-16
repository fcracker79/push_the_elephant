mod stream;
mod pgsql;
mod kafka;
use crate::stream::StreamProducer;
use std::boxed::Box;

fn write_table_to_topic() {
    pgsql::pgsql::PostgreSQLStreamProducer::new("pgurl", "table", "column", "a filter").produce(
        Box::from(kafka::kafka::KafkaStreamConsumer::new(vec!["kafkaurl".to_string()], "a topic", 100))
    );
}
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
