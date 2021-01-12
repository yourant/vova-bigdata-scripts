drop table dwd.dwd_vova_fact_shield_goods;
CREATE EXTERNAL TABLE dwd.dwd_vova_fact_shield_goods
(
    goods_id    STRING,
    region_id   STRING,
    mct_id      STRING,
    shield_type STRING,
    create_time TIMESTAMP
)
    COMMENT '商品屏蔽事实表'
    STORED AS PARQUETFILE;