use crate::kafka;
use crate::common::*;
use crate::pgsql;
use std::time::Duration;
use std::error;
use std::thread;
use std::fmt;
use crate::conf::configuration;
use std::sync::Arc;

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

#[derive(Debug, Clone)]
struct ThreadDiedError;

impl fmt::Display for ThreadDiedError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Thread died unexpectedly")
    }
}

impl error::Error for ThreadDiedError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl <'a> Worker<'a> {
    pub fn run(&self) -> Result<(), Box<error::Error>> {
        let mut consumer = kafka::stream::KafkaStreamConsumer::new(self.kafka_brokers.clone(), self.topic_name, self.buffer_size)?;
        let producer = pgsql::stream::PostgreSQLListenStreamProducer::new(self.pgurl, self.table_name, self.column_name, self.channel, self.notify_timeout_total, self.notify_timeout);
        producer.produce(&mut consumer)?;
        return Ok(());
    }

    pub fn multi_run(yaml_configuration_filename: &str) -> Result<(), Box<error::Error>> {
        let threads_control = Arc::new(true);
        let configurations = Arc::new(configuration::PushTheElephantConfiguration::create_from_yaml_filename(yaml_configuration_filename)?);
        let mut join_handles : Vec<thread::JoinHandle<_>> = Vec::new();
        for i in 0..configurations.len() {
             let cur_thread_control = Arc::clone(&threads_control);
             let cur_configurations = Arc::clone(&configurations);
             join_handles.push(
                 thread::spawn(
                     move || {
                         let mut builder = WorkerBuilder::default();
                         let c = &cur_configurations[i];
                         if let Some(x) = c.pgurl.as_ref() {
                             builder.pgurl(x);
                         }
                         if let Some(x) = c.table_name.as_ref() {
                             builder.table_name(x);
                         }
                         if let Some(x) = c.column_name.as_ref() {
                             builder.column_name(x);
                         }
                         if let Some(x) = c.channel.as_ref() {
                             builder.channel(x);
                         }
                         if let Some(x) = c.topic_name.as_ref() {
                             builder.topic_name(x);
                         }
                         if let Some(x) = c.buffer_size {
                             builder.buffer_size(x);
                         }
                         if let Some(x) = &c.kafka_brokers {
                             builder.kafka_brokers(x.to_vec());
                         }
                         if let Some(x) = c.notify_timeout {
                             builder.notify_timeout(x);
                         }
                         if let Some(x) = c.notify_timeout {
                             builder.notify_timeout_total(x);
                         }
                         builder.build().unwrap().run().unwrap();
                         println!("{:?}", cur_thread_control);
                     }
                 )
             );
        }
        loop {
            let current_living_threads = Arc::strong_count(&threads_control) - 1;
            if current_living_threads < configurations.len() {
                return Err(Box::from(ThreadDiedError{}));
            }
            thread::sleep(Duration::from_secs(5));
        }
    }
}
