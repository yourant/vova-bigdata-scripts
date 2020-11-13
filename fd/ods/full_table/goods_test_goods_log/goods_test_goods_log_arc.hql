CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_test_goods_log_arc
(
    `id`               int COMMENT '自增主键',
    `goods_id`         int COMMENT '商品id',
    `country`          string COMMENT '国家',
    `project`          string COMMENT '组织',
    `platform`         string COMMENT '平台',
    `type_id`          int COMMENT '类型id',
    `old_state`        int COMMENT '旧状态',
    `new_state`        int COMMENT '新状态',
    `msg`              string COMMENT '',
    `admin_name`       string COMMENT '',
    `create_time`      bigint COMMENT '创建时间'
) comment 'vbridal.goods_test_goods_log表'
    PARTITIONED BY (dt STRING )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE
    TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_goods_test_goods_log_arc PARTITION (dt = '${hiveconf:dt}')
select id,
       goods_id,
       country,
       project,
       platform,
       type_id,
       old_state,
       new_state,
       msg,
       admin_name,
       create_time
from tmp.tmp_fd_goods_test_goods_log_full;
