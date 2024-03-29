pub mod stream {
    extern crate postgres;
    use crate::common;
    use std::error;
    use postgres::{Connection, TlsMode};
    use std::boxed::Box;
    use fallible_iterator::FallibleIterator;
    use serde_json::{Value};
    use std::time::{SystemTime, Duration}; 
    use log::{info, debug, error};
    
    /// A StreamProducer for PostGreSQL LISTEN/NOTIFY backed up by standard SQL
    pub struct PostgreSQLListenStreamProducer<'a> {
        url: &'a str,
        table_name: &'a str,
        column_name: &'a str,
        channel: &'a str,
        notify_timeout_total: Duration,
        notify_timeout: Duration
    }

    impl <'a> PostgreSQLListenStreamProducer<'a> {
        /// Returns a StreamProducer For PostGreSQL LISTEN/NOTIFY backed up by standard SQL
        ///
        /// Arguments:
        ///
        /// * `url` - PostGreSQL connection URL
        /// * `table_name` - The table where messages to be sent are kept
        /// * `column_name` - The column in the `table_name` table where the message content
        /// resides
        /// * `notify_timeout_total` - The timeout after which the producer moves data from
        /// PostGreSQL to Kafka by using a standard SQL query and then flushes it. After that, it
        /// starts back listening for notifications.
        /// * `notify_timeout` - The timeout after which the notification system times out. When
        /// this happens, the producer flushes all the data, then starts back listening for
        /// notifications.
        pub fn new(url: &'a str, table_name: &'a str, column_name: &'a str, channel: &'a str, notify_timeout_total: Duration, notify_timeout: Duration) -> PostgreSQLListenStreamProducer<'a> {
            info!(target: "postgres", "Creating PostGreSQL connector for table {:?}, notifications at channel {:?}", table_name, channel);
            PostgreSQLListenStreamProducer{url, table_name, column_name, channel, notify_timeout_total, notify_timeout}
        }
    }
    
    impl <'a> PostgreSQLListenStreamProducer<'a> {
        fn flush_consumer(&self, data_to_delete: &mut Vec<i32>, consumer: &mut impl common::StreamConsumer, conn: &Connection) -> Result<(), Box<error::Error>> {
            info!(target: "postgres", "Flushing consumer");
            consumer.flush()?;
            if !data_to_delete.is_empty() {
                info!(target: "postgres", "Deleting {:?} rows from the table as already pushed", data_to_delete.len());
                conn.execute(
                    &format!(
                        "DELETE FROM {} WHERE id IN ({})", 
                        self.table_name, 
                        data_to_delete.iter().map(|x| x.to_string()).collect::<Vec<String>>().join(",")
                    ), 
                    &[]
                )?;
            }
            data_to_delete.clear();
            return Ok(());
        }
    }
    impl <'a> common::StreamProducer for PostgreSQLListenStreamProducer<'a> {
        fn produce(& self, consumer: &mut impl common::StreamConsumer) -> Result<(), Box<error::Error>> {
            let conn = Connection::connect(self.url, TlsMode::None)?;
            let notifications = conn.notifications();
            let mut it = notifications.timeout_iter(self.notify_timeout);
            conn.execute(&*format!("LISTEN \"{}\"", self.channel.to_string()), &[])?;
            loop {
                info!(target: "postgres", "Fallback read data from buffer table");
                let mut data_to_delete : Vec<i32> = Vec::new();
                let sql = &format!("SELECT id, {} FROM {}", self.column_name, self.table_name);
                for next_row in &conn.query(sql, &[])? {
                    let id: i32 = next_row.get(0);
                    let data: String = next_row.get(1);
                    let bin_data : &[u8] = data.as_bytes();
                    consumer.write(common::SourceElement{id: Box::from("123"), data: Box::from(bin_data)})?;
                    data_to_delete.push(id);
                }
                info!(target: "postgres", "Fallback data pushed messages: {:?}", data_to_delete.len());
                self.flush_consumer(&mut data_to_delete, consumer, &conn)?;
                let start_time = SystemTime::now();
                loop {
                    if start_time.elapsed()? >= self.notify_timeout_total {
                        break;
                    }
                    info!(target: "postgres", "Fallback data pushed messages: {:?}", data_to_delete.len());
                    let a = it.next();
                    match a {
                        Ok(b) => {
                            if let Some(x) = b {
                                let json_payload : Value = serde_json::from_str(&x.payload)?;
                                debug!(target: "postgres", "Received {:?} from PostGreSQL notification", json_payload);
                                let string_payload: &str = match json_payload["payload"].as_str() {
                                    Some(x) => x,
                                    _ => {
                                        return Err(Box::from("No such payload"));
                                    }
                                };
                                let id : i32 = match json_payload["id"].as_i64() {
                                    Some(x) => x as i32,
                                    _ => {
                                        return Err(Box::from("No such ID"));
                                    }
                                };
                                consumer.write(common::SourceElement{id: Box::from("123"), data: Box::from(string_payload.as_bytes())})?;
                                data_to_delete.push(id);
                            } else {
                                debug!(target: "postgres", "Notification timeout expired");
                                self.flush_consumer(&mut data_to_delete, consumer, &conn)?;
                            }
                        },
                        Err(e) => {
                            error!(target: "postgres", "Error attempting to read notifications: {:?}", e);
                            conn.query("", &[])?;
                            continue;
                        }
                    }
                }
                self.flush_consumer(&mut data_to_delete, consumer, &conn)?;
            }
        }
    }
}
