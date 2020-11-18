drop table dwd.dwd_fact_vova_shield_goods;
CREATE TABLE dwd.dwd_fact_vova_shield_goods
(
    goods_id    STRING,
    region_id   STRING,
    mct_id      STRING,
    shield_type STRING,
    create_time TIMESTAMP
)
    COMMENT '商品屏蔽事实表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;