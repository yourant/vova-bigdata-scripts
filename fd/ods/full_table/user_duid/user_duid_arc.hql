CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_user_duid_arc
(
    `id` bigint COMMENT '主键 自增id',
    `user_id` bigint COMMENT '',
    `sp_duid` string COMMENT '',
    `created_time` bigint COMMENT '',
    `last_update_time` bigint COMMENT ''
 )comment '用户id和打点id'
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

INSERT overwrite table ods_fd_vb.ods_fd_user_duid_arc PARTITION (dt='${hiveconf:dt}')
select  
    id,
    user_id,
    sp_duid,
    unix_timestamp(to_utc_timestamp(created_time, 'America/Los_Angeles')) as created_time,
    unix_timestamp(to_utc_timestamp(last_update_time, 'America/Los_Angeles')) as last_update_time
from tmp.tmp_fd_user_duid_full;
