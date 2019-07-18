# Push the elephant

Moves data from PostGreSQL database table to a Apache Kafka topic.  
This may come in handy when you need to have RDBMS transactions that both modify the database and send messages to Apache Kafka.

Introduction
------------
Given a PostGreSQL table with the following structure:

1. an `id` field as a primary key
2. an arbitrary `VARCHAR` field containing what you are expecting to send to Kafka
3. a trigger that notifies inserts using a PostGreSQL channel

When Push The Elephant is in execution, all the data in the above table is +moved+ to a Kafka topic.  
This allows you to write projects that both changes your PostGreSQL data and send Kafka messages in a transactional context.
All you have to do is write a row in the above table within your transaction.

CLI example
-----------

The tools comes with a command line interface:

```
Push the Elephant 0.0.1
Mirko Bonasorte <fcracker79@gmail.com>
Moves data from a PostgreSQL table to Kafka topic using LISTEN/NOTIFY mechanisms

USAGE:
    pte [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -b, --buffer-size <BUFFER_SIZE>
            Kafka buffer size after which messages are written (default: 100)

    -z, --channel-name <CHANNEL_NAME>                    PostGreSQL channel name (default: events.activity)
    -c, --column-name <COLUMN_NAME>                      PostGreSQL Table column name (default: payload)
    -k, --kafka-urls <PG_URL>                            Kafka URLs (default: localhost:9092)
    -l, --log4rs-configuration <LOG4RS_CONFIGURATION>    Log4rs YAML configuration file
    -x, --notify-timeout <NOTIFY_TIMEOUT>                PostGreSQL Listen timeout (ms, default: 3000)
    -X, --notify-timeout-total <NOTIFY_TIMEOUT_TOTAL>
            Timeout after which rows are processed using a standard query (ms, default: 60000)

    -p, --pgurl <PG_URL>                                 PostGreSQL URL (default: postgres://postgres@localhost:5433)
    -t, --table-name <TABLE_NAME>                        PostGreSQL Table name (default: events)
    -w, --topic-name <TOPIC_NAME>                        Kafka topic name (default: events)
```

Library example
---------------
You can also embed the tool in your own Rust project, as follows:

```rust
use stream;
use std::time::Duration;

fn go() {
    stream::WorkerBuilder::default()
        .pgurl("<your PostGreSQL URL>")
        .kafka_brokers(vec!["<your Kafka host>".to_string()])
        .table_name("<your table name>")
        .column_name("<your table column containing the data to be pushed to Kafka>")
        .channel("<PostGreSQL channel for LISTEN/NOTIFY>")
        .notify_timeout(Duration::from_millis(500))
        .notify_timeout_total(Duration::from_millis(60000))
        .build().unwrap().run();
}
```
The `notify-timeout` defines how much time the tool has to wait before it can flush data to Kafka.
The `notify-timeout-total` defines how much time the tool has to wait before it can fallback to a standard SQL query to fetch the data to be moved to Kafka.
The `buffer-size` define how many messages are to be collected before flushing data to Kafka.

PostGreSQL Trigger
------------------
The following script contains an example of a trigger that intercepts all the inserts into the `events` table and sends such rows to the `events.activity` PostGreSQL channel.

```sql
begin;

create or replace function tg_notify_events ()
 returns trigger
 language plpgsql
as $$
declare
  channel text := TG_ARGV[0];
begin
  PERFORM (
     with new_row(id, payload) as (select NEW.id, NEW.payload)
     select pg_notify(channel, row_to_json(new_row)::text)
       from new_row
  );
  RETURN NULL;
end;
$$;

CREATE TRIGGER notify_events
         AFTER INSERT
            ON events
      FOR EACH ROW
       EXECUTE PROCEDURE tg_notify_events('events.activity');

commit;
```

