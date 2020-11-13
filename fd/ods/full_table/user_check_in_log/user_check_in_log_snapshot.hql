CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_user_check_in_log
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
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE
    TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_user_check_in_log
select `(dt)?+.+`
from ods_fd_vb.ods_fd_user_check_in_log_arc
where dt = '${hiveconf:dt}';

