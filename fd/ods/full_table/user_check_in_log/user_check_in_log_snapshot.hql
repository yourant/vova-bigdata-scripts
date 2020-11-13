CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_user_check_in_log
(
    `id`               bigint COMMENT '',
    `user_id`          bigint COMMENT '',
    `time`             string COMMENT '',
    `date`             string COMMENT '',
    `project`          string COMMENT '',
    `points`           bigint COMMENT '',
    `type`             string COMMENT '',
    `coupon_config_id` bigint COMMENT '',
    `extra_points`     bigint COMMENT ''
) comment '用户签到日志'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_user_check_in_log
select `(dt)?+.+`
from ods_fd_vb.ods_fd_user_check_in_log_arc
where dt = '${hiveconf:dt}';

