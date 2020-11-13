CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_test_goods_log
(
    `id`               bigint COMMENT '自增主键',
    `goods_id`         bigint COMMENT '商品id',
    `country`          string COMMENT '国家',
    `project`          string COMMENT '组织',
    `platform`         string COMMENT '平台',
    `type_id`          bigint COMMENT '类型id',
    `old_state`        bigint COMMENT '旧状态',
    `new_state`        bigint COMMENT '新状态',
    `msg`              string COMMENT '',
    `admin_name`       string COMMENT '',
    `create_time`      bigint COMMENT '创建时间'
) comment 'vbridal.goods_test_goods_log表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_goods_test_goods_log
select `(dt)?+.+`
from ods_fd_vb.ods_fd_goods_test_goods_log_arc
where dt = '${hiveconf:dt}';
