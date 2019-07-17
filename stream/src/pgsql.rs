

pub mod stream {
    extern crate postgres;
    use crate::stream;
    use postgres::{Connection, TlsMode};
    use std::boxed::Box;
    use fallible_iterator::FallibleIterator;
    use serde_json::{Value};
    use std::time::{SystemTime, Duration}; 
    use log::{info, debug, error};

    pub struct PostgreSQLListenStreamProducer<'a> {
        url: &'a str,
        table_name: &'a str,
        column_name: &'a str,
        channel: &'a str,
        notify_timeout_total: Duration,
        notify_timeout: Duration
    }

    impl <'a> PostgreSQLListenStreamProducer<'a> {
        pub fn new(url: &'a str, table_name: &'a str, column_name: &'a str, channel: &'a str, notify_timeout_total: Duration, notify_timeout: Duration) -> PostgreSQLListenStreamProducer<'a> {
            info!(target: "postgres", "Creating PostGreSQL connector for table {:?}, notifications at channel {:?}", table_name, channel);
            PostgreSQLListenStreamProducer{url, table_name, column_name, channel, notify_timeout_total, notify_timeout}
        }
    }
    
    impl <'a> PostgreSQLListenStreamProducer<'a> {
        fn flush_consumer(&self, data_to_delete: &mut Vec<i32>, consumer: &mut impl stream::StreamConsumer, conn: &Connection) {
            info!(target: "postgres", "Flushing consumer");
            consumer.flush();
            if !data_to_delete.is_empty() {
                info!(target: "postgres", "Deleting {:?} rows from the table as already pushed", data_to_delete.len());
                conn.execute(
                    &format!(
                        "DELETE FROM {} WHERE id IN ({})", 
                        self.table_name, 
                        data_to_delete.iter().map(|x| x.to_string()).collect::<Vec<String>>().join(",")
                    ), 
                    &[]
                ).unwrap();
            }
                data_to_delete.clear();
        }
    }
    impl <'a> stream::StreamProducer for PostgreSQLListenStreamProducer<'a> {
        fn produce(& self, consumer: &mut impl stream::StreamConsumer) {
            let conn = Connection::connect(self.url, TlsMode::None).unwrap();
            let notifications = conn.notifications();
            let mut it = notifications.timeout_iter(self.notify_timeout);
            conn.execute(&*format!("LISTEN \"{}\"", self.channel.to_string()), &[]).unwrap();
            loop {
                info!(target: "postgres", "Fallback read data from buffer table");
                let mut data_to_delete : Vec<i32> = Vec::new();
                let sql = &format!("SELECT id, {} FROM {}", self.column_name, self.table_name);
                for next_row in &conn.query(sql, &[]).unwrap() {
                    let id: i32 = next_row.get(0);
                    let data: String = next_row.get(1);
                    let bin_data : &[u8] = data.as_bytes();
                    consumer.write(stream::SourceElement{id: Box::from("123"), data: Box::from(bin_data)});
                    data_to_delete.push(id);
                }
                info!(target: "postgres", "Fallback data pushed messages: {:?}", data_to_delete.len());
                self.flush_consumer(&mut data_to_delete, consumer, &conn);
                let start_time = SystemTime::now();
                loop {
                    if start_time.elapsed().unwrap() >= self.notify_timeout_total {
                        break;
                    }
                    info!(target: "postgres", "Fallback data pushed messages: {:?}", data_to_delete.len());
                    let a = it.next();
                    match a {
                        Ok(b) => {
                            if let Some(x) = b {
                                let json_payload : Value = serde_json::from_str(&x.payload).unwrap();
                                debug!(target: "postgres", "Received {:?} from PostGreSQL notification", json_payload);
                                let string_payload: &str = json_payload["payload"].as_str().unwrap();
                                let id : i32 = json_payload["id"].as_i64().unwrap() as i32;
                                consumer.write(stream::SourceElement{id: Box::from("123"), data: Box::from(string_payload.as_bytes())});
                                data_to_delete.push(id);
                            } else {
                                debug!(target: "postgres", "Notification timeout expired");
                                self.flush_consumer(&mut data_to_delete, consumer, &conn);
                            }
                        },
                        Err(e) => {
                            error!(target: "postgres", "Error attempting to read notifications: {:?}", e);
                            conn.query("", &[]).unwrap();
                            continue;
                        }
                    }
                }
                self.flush_consumer(&mut data_to_delete, consumer, &conn);
            }
        }
    }
}
