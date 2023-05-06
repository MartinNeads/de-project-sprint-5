alter table dds.dm_orders 
ADD Column courier_id integer NOT Null default 0,
ADD CONSTRAINT dm_orders_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dm_couriers(id);