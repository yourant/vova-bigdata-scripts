CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_test_preorder_result
(
    id               bigint comment '自增主键',
    project          string comment '组织',
    virtual_goods_id bigint comment '虚拟商品id',
    goods_id         bigint comment '商品id',
    preorder_plan_id bigint comment '预售计划id',
    cat_id           bigint comment '品类id',
    status           bigint comment '测款状态',
    create_time      bigint comment '创建时间',
    update_time      bigint comment '更新时间'
) comment '从vbridal同步过来的测款结果表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE
    TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_goods_test_preorder_result
select `(dt)?+.+`
from ods_fd_vb.ods_fd_goods_test_preorder_result_arc
where dt = '${hiveconf:dt}';
