CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_test_pipeline
(
    pipeline_id bigint comment '',
    country     string comment '国家',
    project     string comment '组织',
    platform    string comment '平台',
    deleted     string comment '',
    create_time string comment '创建时间',
    update_time string comment '更新时间'
) comment ''
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE
    TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_goods_test_pipeline
select `(dt)?+.+`
from ods_fd_vb.ods_fd_goods_test_pipeline_arc
where dt = '${hiveconf:dt}';
