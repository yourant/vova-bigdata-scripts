DROP TABLE dwb.dwb_zq_user_cohort_daily;
CREATE TABLE IF NOT EXISTS dwb.dwb_zq_user_cohort_daily
(
    event_date       date COMMENT '启动日期',
    datasource       string,
    region_code      string,
    platform         string,
    original_channel string,
    is_new_active    string,
    dau_d0           bigint,
    dau_d1           bigint,
    dau_d7           bigint,
    dau_d30          bigint
) COMMENT 'fn用户日留存报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


DROP TABLE dwb.dwb_zq_user_cohort_weekly;
CREATE TABLE IF NOT EXISTS dwb.dwb_zq_user_cohort_weekly
(
    event_date       date COMMENT '启动日期',
    datasource       string,
    region_code      string,
    platform         string,
    original_channel string,
    is_new_active    string,
    wau_w0           bigint,
    wau_w1           bigint,
    wau_w2           bigint,
    wau_w3           bigint,
    wau_quarterly    bigint
) COMMENT 'fn用户周留存报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_zq_user_repurchase_daily;
CREATE TABLE IF NOT EXISTS dwb.dwb_zq_user_repurchase_daily
(
    event_date       date COMMENT '启动日期',
    datasource       string,
    region_code      string,
    platform         string,
    original_channel string,
    is_new_active    string,
    repurchase_d0    bigint,
    repurchase_d1    bigint,
    repurchase_w1    bigint,
    repurchase_m1    bigint,
    repurchase_q1    bigint
) COMMENT 'fn用户日复购报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_zq_user_repurchase_weekly;
CREATE TABLE IF NOT EXISTS dwb.dwb_zq_user_repurchase_weekly
(
    event_date       date COMMENT '启动日期',
    datasource       string,
    region_code      string,
    platform         string,
    original_channel string,
    is_new_active    string,
    repurchase_w0    bigint,
    repurchase_w1    bigint,
    repurchase_w2    bigint,
    repurchase_w3    bigint,
    repurchase_quarterly    bigint
) COMMENT 'fn用户周复购报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


