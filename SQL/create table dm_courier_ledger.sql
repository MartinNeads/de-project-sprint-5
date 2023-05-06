create table cdm.dm_courier_ledger (
    id serial NOT NULL,
    courier_id varchar NOT NULL,
    courier_name varchar NOT NULL,
    settlement_year integer NOT NULL CHECK (( (settlement_year >= 2022) AND (settlement_year < 2500))),
    settlement_month integer NOT NULL CHECK (( (settlement_month >= 1) AND (settlement_month <= 12))),
    orders_count integer NOT NULL,
    orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
    rate_avg numeric(14, 2) NOT NULL DEFAULT 0,
    order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
    courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0,
    courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0,
    courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
    CONSTRAINT PK_dm_courier_ledger_id PRIMARY KEY (id),
    CONSTRAINT dm_courier_ledger_orders_count_check CHECK ((orders_count >= 0)),
    CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
    CONSTRAINT dm_courier_ledger_rate_avg_check CHECK ((rate_avg >= (0)::numeric)),
    CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
    CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum >= (0)::numeric)),
    CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK ((courier_tips_sum >= (0)::numeric)),
    CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK ((courier_reward_sum >= (0)::numeric)),
    CONSTRAINT dm_courier_ledger_courier_id_settlement_date UNIQUE (courier_id, settlement_year, settlement_month)
);