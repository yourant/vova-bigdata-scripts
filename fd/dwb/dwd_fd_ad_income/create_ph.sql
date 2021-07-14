CREATE external TABLE IF NOT EXISTS dwb.dwb_ph_income_order
(
    cat_name       STRING,
    day_value         STRING,
    day_rate           STRING,
    month_value         STRING,
    month_rate      STRING,
    rn      int
)  PARTITIONED BY(pt string)
row format delimited fields terminated by '\001'  STORED AS PARQUETFILE
;

CREATE external TABLE IF NOT EXISTS dwb.dwb_ph_income_out_stock
(
    cat_name       STRING,
    day_value         STRING,
    day_rate           STRING,
    month_value         STRING,
    month_rate      STRING,
    rn      int
)  PARTITIONED BY(pt string)
row format delimited fields terminated by '\001'  STORED AS PARQUETFILE
;

CREATE external TABLE IF NOT EXISTS tmp.tmp_ph_income_01
(
    day_order_cnt         int,
    day_gmv           decimal(15,4),
    day_bonus         decimal(15,4),
    day_shou_express      decimal(15,4),

    mon_order_cnt         int,
    mon_gmv           decimal(15,4),
    mon_bonus         decimal(15,4),
    mon_shou_express      decimal(15,4),

    day_before_month_rate decimal(15,4),
    month_before_month_rate decimal(15,4),
    union_cost_day decimal(15,4),
    unit_cost_month decimal(15,4),
    order_amount_day decimal(15,4),
    order_amount_month decimal(15,4)
)
row format delimited fields terminated by '\001'  STORED AS PARQUETFILE
;

CREATE external TABLE IF NOT EXISTS tmp.tmp_ph_income_02
(
    ad_cost_day         decimal(15,4),
    ad_cost_month           decimal(15,4)
)
row format delimited fields terminated by '\001'  STORED AS PARQUETFILE
;

CREATE external TABLE IF NOT EXISTS tmp.tmp_ph_income_03
(
    day_order_cnt         int,
    day_gmv           decimal(15,4),
    day_bonus         decimal(15,4),
    day_shou_express      decimal(15,4),

    mon_order_cnt         int,
    mon_gmv           decimal(15,4),
    mon_bonus         decimal(15,4),
    mon_shou_express      decimal(15,4),

    day_before_month_rate decimal(15,4),
    month_before_month_rate decimal(15,4),
    union_cost_day decimal(15,4),
    unit_cost_month decimal(15,4),
    order_amount_day decimal(15,4),
    order_amount_month decimal(15,4),
    should_express_amount_day decimal(15,4),
    should_express_amount_month decimal(15,4),
    order_dim_day int,
    order_dim_month int,
    ad_cost_day decimal(15,4),
    ad_cost_month decimal(15,4)
)
row format delimited fields terminated by '\001'  STORED AS PARQUETFILE
;