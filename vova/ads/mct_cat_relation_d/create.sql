--商品维度汇总表
drop table ads.ads_vova_mct_cat_relation;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_mct_cat_relation
(
    mct_id                    BIGINT COMMENT '小时',
    first_cat_id              BIGINT COMMENT '一级品类id',
    goods_id                  BIGINT COMMENT '商品ID',
    virtual_goods_id          BIGINT COMMENT '商品虚拟ID',
    cat_id                    BIGINT COMMENT '品类ID',
    second_cat_id             BIGINT COMMENT '二级品类ID'
) COMMENT '商家品类关系表'
 PARTITIONED BY ( pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
