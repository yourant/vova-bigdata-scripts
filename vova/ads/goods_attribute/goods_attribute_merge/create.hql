drop table ads.ads_vova_goods_attribute_merge;
create
    external table if not exists ads.ads_vova_goods_attribute_merge
(
    goods_id      bigint comment '商品ID',
    cat_attr_id   int comment '品类ID',
    attr_key      string comment '属性名称',
    attr_value    string comment '属性值',
    first_cat_id  int comment '一级品类ID',
    second_cat_id int comment '二级品类ID',
    third_cat_id  int COMMENT '三级品类ID',
    fourth_cat_id int comment '四级品类ID',
    cat_id        int comment '最小级品类ID',
    brand_id      bigint comment 'BRAND ID',
    goods_sn      string comment '商品SN'
) COMMENT '商品属性整合表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
