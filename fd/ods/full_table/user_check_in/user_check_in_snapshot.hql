CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_user_check_in
(
    `id`         bigint COMMENT '',
    `user_id`    bigint COMMENT '',
    `count`      bigint COMMENT '',
    `last_date`  string COMMENT '',
    `per_count`  bigint COMMENT '',
    `full_count` bigint COMMENT ''
) comment '用户签到'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_user_check_in
select `(dt)?+.+`
from ods_fd_vb.ods_fd_user_check_in_arc
where dt = '${hiveconf:dt}';
