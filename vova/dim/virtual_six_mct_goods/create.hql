--商品维度汇总表
drop table dim.dim_vova_virtual_six_mct_goods;
CREATE external TABLE IF NOT EXISTS dim.dim_vova_virtual_six_mct_goods
(
    goods_id         BIGINT COMMENT '商品ID',
    virtual_goods_id BIGINT COMMENT '商品虚拟ID',
    mct_id           BIGINT COMMENT '商品所属商家',
    mct_name         string COMMENT '商品所属商家',
    first_cat_id     BIGINT COMMENT '商品一级类目',
    second_cat_id    BIGINT COMMENT '商品二级类目',
    cat_id           BIGINT COMMENT '商品类目ID',
    brand_id         BIGINT COMMENT '侵权商品',
    group_id         BIGINT COMMENT '原始分组id，不是低价商品组的分组id，没有分到组则为-1'
) COMMENT '商家分级流量表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;