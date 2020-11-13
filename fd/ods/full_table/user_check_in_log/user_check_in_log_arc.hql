CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_user_check_in_log_arc
(
    `id`               int COMMENT '',
    `user_id`          int COMMENT '',
    `time`             string COMMENT '',
    `date`             string COMMENT '',
    `project`          string COMMENT '',
    `points`           int COMMENT '',
    `type`             string COMMENT '',
    `coupon_config_id` int COMMENT '',
    `extra_points`     int COMMENT ''
) comment '用户签到日志'
    PARTITIONED BY (dt STRING )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE
    TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_user_check_in_log_arc PARTITION (dt = '${hiveconf:dt}')
select 
    id,
    user_id,
    `time`,
    `date`,    
    project,
    points,
    type,
    coupon_config_id,
    extra_points
from tmp.tmp_fd_user_check_in_log_full;
