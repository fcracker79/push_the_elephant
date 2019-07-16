

pub mod pgsql {
    extern crate postgres;
    use crate::stream;
    use postgres::{Connection, TlsMode};
    use std::boxed::Box;
    use fallible_iterator::FallibleIterator;
    use serde_json::{Result, Value};


    pub struct PostgreSQLStreamProducer<'a> {
        url: &'a str,
        table_name: &'a str,
        column_name: &'a str,
        filter: &'a str
    }
    
    impl <'a> PostgreSQLStreamProducer<'a> {
        pub fn new(url: &'a str, table_name: &'a str, column_name: &'a str, filter: &'a str) -> PostgreSQLStreamProducer<'a> {
            PostgreSQLStreamProducer{url: url, table_name: table_name, column_name: column_name, filter: filter}
        }
    }
    impl <'a> stream::StreamProducer for PostgreSQLStreamProducer<'a> {
        fn produce(& self, consumer: &mut impl stream::StreamConsumer) {
            let sql = &format!("SELECT {} FROM {}  WHERE {}", self.column_name, self.table_name, self.filter);
            let conn = Connection::connect(self.url, TlsMode::None).unwrap();
            for next_row in &conn.query(sql, &[]).unwrap() {
                let data: String = next_row.get(0);
                let bin_data : &[u8] = data.as_bytes();
                consumer.write(stream::SourceElement{id: Box::from("123"), data: Box::from(bin_data)});
            }
            consumer.flush();
        }
    }

    pub struct PostgreSQLListenStreamProducer<'a> {
        url: &'a str,
        table_name: &'a str,
        column_name: &'a str,
        channel: &'a str
    }

    impl <'a> PostgreSQLListenStreamProducer<'a> {
        pub fn new(url: &'a str, table_name: &'a str, column_name: &'a str, channel: &'a str) -> PostgreSQLListenStreamProducer<'a> {
            PostgreSQLListenStreamProducer{url: url, table_name: table_name, column_name: column_name, channel: channel}
        }
    }

    impl <'a> stream::StreamProducer for PostgreSQLListenStreamProducer<'a> {
        fn produce(& self, consumer: &mut impl stream::StreamConsumer) {
            let conn = Connection::connect(self.url, TlsMode::None).unwrap();
            let notifications = conn.notifications();
            let mut it = notifications.iter();
            conn.execute(&*format!("LISTEN \"{}\"", self.channel.to_string()), &[]).unwrap();
            loop {
                let mut data_to_delete : Vec<i64> = Vec::new();
                let sql = &format!("SELECT id, {} FROM {}", self.column_name, self.table_name);
                for next_row in &conn.query(sql, &[]).unwrap() {
                    let id: i32 = next_row.get(0);
                    let data: String = next_row.get(1);
                    let bin_data : &[u8] = data.as_bytes();
                    consumer.write(stream::SourceElement{id: Box::from("123"), data: Box::from(bin_data)});
                    data_to_delete.push(id as i64);
                }
                consumer.flush();
                if !data_to_delete.is_empty() {
                    conn.execute("DELETE FROM {} WHERE id IN ($1)", &[&data_to_delete]).unwrap();
                }
                data_to_delete.clear();
                for i in 0..1000 {
                    let a = it.next();
                    match a {
                        Ok(b) => {
                            if let Some(x) = b {
                                let json_payload : Value = serde_json::from_str(&x.payload).unwrap();
                                let string_payload: String = json_payload["payload"].to_string();
                                let id : i64 = json_payload["id"].as_i64().unwrap();
                                consumer.write(stream::SourceElement{id: Box::from("123"), data: Box::from(string_payload.as_bytes())});
                                data_to_delete.push(id);
                            }
                        },
                        _ => {
                            conn.query("", &[]).unwrap();
                            continue;
                        }
                    }
                }
                if !data_to_delete.is_empty() {
                    conn.execute("DELETE FROM {} WHERE id IN ($1)", &[&data_to_delete]).unwrap();
                }
                data_to_delete.clear();
            }
        }
    }
}
