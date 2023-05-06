alter table dds.fct_product_sales
add column tip_sum numeric(14,2) not null DEFAULT 0,
add CONSTRAINT fct_product_sales_tip_sum_check CHECK ((tip_sum >= (0)::numeric));