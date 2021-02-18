DROP TABLE dwb.dwb_ac_web_cohort;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_ac_web_cohort
(
    action_date                  date,
    datasource                   string COMMENT 'datasource',
    region_code                  string,
    is_activate                  string,
    is_new_user                  string,
    medium                       string,
    source                       string,
    next_0_cnt                   bigint,
    next_1_cnt                   bigint,
    next_3_cnt                   bigint,
    next_7_cnt                   bigint,
    next_28_cnt                  bigint,
    interval_1_cnt               bigint,
    interval_3_cnt               bigint,
    interval_7_cnt               bigint,
    interval_28_cnt              bigint,
    is_new_reg_time              string COMMENT 'is_new_reg_time',
    is_new_register_success_time string COMMENT 'is_new_register_success_time'
) COMMENT 'web留存' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_ac_web_ltv;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_ac_web_ltv
(
    action_date                  date,
    region_code                  string,
    is_new_user                  string,
    medium                       string,
    source                       string,
    cur_paid_uv                  bigint,
    cur_bonus                    decimal(15, 2),
    cur_gmv                      decimal(15, 2),
    cur_order_amount             decimal(15, 2),
    three_paid_uv                bigint,
    three_bonus                  decimal(15, 2),
    three_gmv                    decimal(15, 2),
    three_order_amount           decimal(15, 2),
    seven_paid_uv                bigint,
    seven_bonus                  decimal(15, 2),
    seven_gmv                    decimal(15, 2),
    seven_order_amount           decimal(15, 2),
    thirty_paid_uv               bigint,
    thirty_bonus                 decimal(15, 2),
    thirty_gmv                   decimal(15, 2),
    thirty_order_amount          decimal(15, 2),
    half_paid_uv                 bigint,
    half_bonus                   decimal(15, 2),
    half_gmv                     decimal(15, 2),
    half_order_amount            decimal(15, 2),
    dau                          bigint,
    is_new_reg_time              string COMMENT 'is_new_reg_time',
    is_new_register_success_time string COMMENT 'is_new_register_success_time'

) COMMENT 'web_ltv' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
