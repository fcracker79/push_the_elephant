use push_the_elephant;
extern crate clap;
use clap::{Arg, App};
use std::time::Duration;
extern crate log4rs;
use log;

fn main() {
    let matches = App::new("Push the Elephant")
                          .version("0.0.1")
                          .author("Mirko Bonasorte <fcracker79@gmail.com>")
                          .about("Moves data from a PostgreSQL table to Kafka topic using LISTEN/NOTIFY mechanisms")
                          .arg(Arg::with_name("pg_url")
                               .short("-p")
                               .long("pgurl")
                               .value_name("PG_URL")
                               .help("PostGreSQL URL (default: postgres://postgres@localhost:5433)")
                               .takes_value(true))
                          .arg(Arg::with_name("kafka_urls")
                               .short("-k")
                               .long("kafka-urls")
                               .value_name("PG_URL")
                               .help("Kafka URLs (default: localhost:9092)")
                               .takes_value(true))
                          .arg(Arg::with_name("table_name")
                               .short("-t")
                               .long("table-name")
                               .value_name("TABLE_NAME")
                               .help("PostGreSQL Table name (default: events)")
                               .takes_value(true))
                          .arg(Arg::with_name("column_name")
                               .short("-c")
                               .long("column-name")
                               .value_name("COLUMN_NAME")
                               .help("PostGreSQL Table column name (default: payload)")
                               .takes_value(true))
                          .arg(Arg::with_name("channel")
                               .short("-z")
                               .long("channel-name")
                               .value_name("CHANNEL_NAME")
                               .help("PostGreSQL channel name (default: events.activity)")
                               .takes_value(true))
                          .arg(Arg::with_name("topic")
                               .short("-w")
                               .long("topic-name")
                               .value_name("TOPIC_NAME")
                               .help("Kafka topic name (default: events)")
                               .takes_value(true))
                          .arg(Arg::with_name("buffer_size")
                               .short("-b")
                               .long("buffer-size")
                               .value_name("BUFFER_SIZE")
                               .help("Kafka buffer size after which messages are written (default: 100)")
                               .takes_value(true))
                          .arg(Arg::with_name("notify_timeout")
                               .short("-x")
                               .long("notify-timeout")
                               .value_name("NOTIFY_TIMEOUT")
                               .help("PostGreSQL Listen timeout (ms, default: 3000)")
                               .takes_value(true))
                          .arg(Arg::with_name("notify_timeout_total")
                               .short("-X")
                               .long("notify-timeout-total")
                               .value_name("NOTIFY_TIMEOUT_TOTAL")
                               .help("Timeout after which rows are processed using a standard query (ms, default: 60000)")
                               .takes_value(true))
                          .arg(Arg::with_name("log4rs_file")
                               .short("-l")
                               .long("log4rs-configuration")
                               .value_name("LOG4RS_CONFIGURATION")
                               .help("Log4rs YAML configuration file")
                               .takes_value(true))
                          .get_matches();
    
    if let Some(log4rs_file) = matches.value_of("log4rs_file") {
        println!("Configuring logger using {}", log4rs_file);
        log4rs::init_file(log4rs_file, Default::default()).unwrap();
    }
    let mut builder = push_the_elephant::WorkerBuilder::default();
    if let Some(pgurl) = matches.value_of("pg_url") {
        builder.pgurl(pgurl);
    }
    if let Some(kafka_urls) = matches.value_of("kafka_urls") {
        builder.kafka_brokers(kafka_urls.split(",").map(|x| x.trim().to_string()).collect());
    }
    if let Some(table_name) = matches.value_of("table_name") {
        builder.table_name(table_name);
    }
    if let Some(column_name) = matches.value_of("column_name") {
        builder.column_name(column_name);
    }
    if let Some(channel) = matches.value_of("channel") {
        builder.channel(channel);
    }
    if let Some(buffer_size) = matches.value_of("buffer_size") {
        builder.buffer_size(buffer_size.parse().unwrap());
    }
    if let Some(notify_timeout) = matches.value_of("notify_timeout") {
        builder.notify_timeout(Duration::from_millis(notify_timeout.parse().unwrap()));
    }
    if let Some(notify_timeout_total) = matches.value_of("notify_timeout_total") {
        builder.notify_timeout_total(Duration::from_millis(notify_timeout_total.parse().unwrap()));
    }
    let worker = builder.build().unwrap();
    log::info!(target: "cli", "Running worker {:?}", worker);
    worker.run();
}
