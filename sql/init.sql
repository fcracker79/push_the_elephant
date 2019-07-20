begin;

create table EVENTS(id SERIAL primary key, payload varchar not null);

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
