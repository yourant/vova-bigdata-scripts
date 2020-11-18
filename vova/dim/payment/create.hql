drop table dim.dim_vova_payment;
CREATE TABLE IF NOT EXISTS dim.dim_vova_payment
(
    datasource     string comment '数据平台',
    payment_id     bigint,
    payment_code   string,
    payment_name   string,
    payment_config string,
    acct_name      string,
    disabled       bigint,
    is_cod         bigint,
    is_gc          bigint
) COMMENT '支付维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

