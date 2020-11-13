CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_test_pipeline_arc
(
    pipeline_id bigint comment '',
    country     string comment '国家',
    project     string comment '组织',
    platform    string comment '平台',
    deleted     string comment '',
    create_time string comment '创建时间',
    update_time string comment '更新时间'
) comment ''
    PARTITIONED BY (dt STRING )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE;


INSERT overwrite table ods_fd_vb.ods_fd_goods_test_pipeline_arc PARTITION (dt = '${hiveconf:dt}')
select pipeline_id,
       country,
       project,
       platform,
       deleted,
       create_time,
       last_update_time
from tmp.tmp_fd_goods_test_pipeline_full;
