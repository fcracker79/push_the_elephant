use push_the_elephant;
use std::thread;
use rand::{thread_rng, Rng};
use std::collections;
use std::sync;
use kafka::consumer::Consumer;
use retry;
use retry::delay;
use postgres;

#[test]
fn messages_must_be_pushed() {
    let conn = postgres::Connection::connect("postgres://push_the_elephant:push_the_elephant@localhost:5432/push_the_elephant", postgres::TlsMode::None).unwrap();
    let expected_messages : Vec<String> = (0..100)
        .map(|_| rand::thread_rng().sample_iter(&rand::distributions::Alphanumeric).take(30).collect::<String>())
        .collect();
    expected_messages.iter().for_each(|x| {
        conn.execute("INSERT INTO events(payload) VALUES($1)", &[x]).unwrap();
    });

    let pte_handle = thread::Builder::new().name("Push The Button Test Runner".to_string())
        .spawn(move || {
            push_the_elephant::WorkerBuilder::default()
                .pgurl("postgres://push_the_elephant:push_the_elephant@localhost:5432/push_the_elephant")
                .kafka_brokers(vec!("localhost:29092".to_string()))
                .build().unwrap().run().unwrap();
        }).unwrap();
    let current_messages = sync::Arc::new(sync::Mutex::new(collections::HashSet::new()));
    let thread_current_messages = sync::Arc::clone(&current_messages);
    let kafka_receiver_handle = thread::Builder::new().name("Kafka Test receiver".to_string())
        .spawn(move || {
            let mut consumer = match retry::retry(
                delay::Fixed::from_millis(1000).take(10), 
                || {
                    match Consumer::from_hosts(vec!("localhost:29092".to_string()))
                        .with_topic("events".to_string())
                        .with_group("test_events".to_string())
                        .with_fallback_offset(kafka::client::FetchOffset::Earliest)
                        .with_offset_storage(kafka::client::GroupOffsetStorage::Kafka)
                        .create() {
                            Ok(c) => retry::OperationResult::Ok(c),
                            _ => retry::OperationResult::Retry(())
                        }
                }).unwrap();
            loop {
                let mss = consumer.poll().unwrap();
                if mss.is_empty() {
                    continue;
                }

                for ms in mss.iter() {
                    for m in ms.messages() {
                        thread_current_messages.lock().unwrap().insert(String::from_utf8(m.value.to_vec()).unwrap());
                    }
                    let _ = consumer.consume_messageset(ms);
                }
                consumer.commit_consumed().unwrap();
            }
        }).unwrap();
    let test_result = retry::retry(delay::Fixed::from_millis(1000).take(10), || {
        if expected_messages.iter().all(|x| current_messages.lock().unwrap().contains(x)) {
            return retry::OperationResult::Ok(());
        }
        return retry::OperationResult::Retry(());
    });
    /*
    pte_handle.thread().interrupt();
    kafka_receiver_handle.thread().interrupt();
    pte_handle.join();
    kafka_receiver_handle.join();
    */
    match test_result {
        Ok(_) => (),
        _ => assert!(false)
    }
}
