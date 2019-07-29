use crate::kafka;
use crate::common::*;
use crate::pgsql;
use std::time::Duration;
use std::error;
use std::thread;
use crate::conf::configuration;

#[derive(Debug)]
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
    pub fn run(&self) -> Result<(), Box<error::Error>> {
        let mut consumer = kafka::stream::KafkaStreamConsumer::new(self.kafka_brokers.clone(), self.topic_name, self.buffer_size)?;
        let producer = pgsql::stream::PostgreSQLListenStreamProducer::new(self.pgurl, self.table_name, self.column_name, self.channel, self.notify_timeout_total, self.notify_timeout);
        producer.produce(&mut consumer)?;
        return Ok(());
    }

    pub fn multi_run(yaml_configuration_filename: &str) -> Result<(), Box<error::Error>> {
        let configurations = configuration::PushTheElephantConfiguration::create_from_yaml_filename(yaml_configuration_filename)?;
        let mut workers = Vec::new();
        for configuration in configurations.iter() {
             let mut builder = WorkerBuilder::default();
             if let Some(x) = configuration.pgurl.as_ref() {
                 builder.pgurl(x);
             }
             workers.push(builder.build()?);
        }
        let join_handles = workers.into_iter().map(|w| thread::spawn(move || w.run().unwrap())).collect::<Vec<thread::JoinHandle<_>>>();
        // TODO
        for join_handle in join_handles {
            join_handle.join();
        }
        Ok(())
    }
}
