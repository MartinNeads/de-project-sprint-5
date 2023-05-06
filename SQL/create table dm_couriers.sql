drop table if exists dm_couriers;
create table dm_couriers (
id serial not null primary key,
courier_id varchar not null,
courier_name varchar not null);