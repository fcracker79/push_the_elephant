use stream;

fn main() {
    stream::WorkerBuilder::default()
        .pgurl("postgres://push_the_elephant:push_the_elephant@localhost:5432/push_the_elephant")
        .kafka_brokers(vec!("localhost:29092".to_string()))
        .build().unwrap().run();
}
