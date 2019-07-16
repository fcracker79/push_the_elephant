
pub mod pgsql {
    extern crate postgres;
    use crate::stream;
    use postgres::{Connection, TlsMode};
    use std::boxed::Box;

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
}
