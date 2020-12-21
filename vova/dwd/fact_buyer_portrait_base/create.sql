drop table if exists dwd.dwd_vova_fact_buyer_portrait_base;
CREATE TABLE dwd.dwd_vova_fact_buyer_portrait_base
(
    datasource   string comment '数据平台',
    buyer_id     bigint COMMENT '买家编码',
    tag_id       string COMMENT '标签id',
    tag_name     string COMMENT '标签名称',
    cnt          bigint COMMENT '行为次数',
    first_cat_id bigint COMMENT '一级类目标签类型',
    act_type_id  bigint COMMENT '行为标签类型'
) COMMENT '用户画像基础事实表'
    partitioned by (pt string)
    row format delimited fields terminated by '\001'
        null defined as ''
    stored as parquet;