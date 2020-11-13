CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_test_goods
(
    `id`                 int COMMENT '自增主键',
    `goods_id`           int COMMENT '商品id',
    `pipeline_id`        int COMMENT '线程id',
    `state`              int COMMENT '状态',
    `type_id`            int COMMENT '类型id',
    `create_time`        bigint COMMENT '创建时间',
    `result`             int COMMENT '测款结果',
    `ctr_checked`        int COMMENT '',
    `reason`             string COMMENT '原因',
    `production_reached` int COMMENT '产品到达',
    `goods_type`         string COMMENT '商品类型',
    `goods_source`       string COMMENT '商品来源',
    `test_count`         int COMMENT '测试次数',
    `last_update_time`   bigint COMMENT '最后更新时间',
    `admin_name`         string COMMENT '',
    `test_time`          bigint comment '入测时间',
    `test_type`          int COMMENT '',
    `is_auto`            int COMMENT '',
    `type_name`          int COMMENT ''
) comment 'vbridal.goods_test_goods表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE
    TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_goods_test_goods
select `(dt)?+.+`
from ods_fd_vb.ods_fd_goods_test_goods_arc
where dt = '${hiveconf:dt}';
