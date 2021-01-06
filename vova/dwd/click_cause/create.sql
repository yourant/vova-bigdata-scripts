CREATE TABLE dwd.dwd_vova_fact_goods_click_cause
(
    buyer_id   BIGINT COMMENT 'd_买家id',
    goods_id   BIGINT COMMENT 'd_商品id',
    click_time STRING COMMENT 'd_点击时间',
    page_code  STRING COMMENT 'i_页面code',
    list_type  STRING COMMENT 'i_list_type',
    enter_ts   STRING COMMENT 'i_详情页停留开始时间',
    leave_ts   STRING COMMENT 'i_详情页停留结束时间',
    stay_time  BIGINT COMMENT 'i_详情页停留时长',
    session_id STRING COMMENT 'i_session_id'
)
    COMMENT '商品点击归因表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;