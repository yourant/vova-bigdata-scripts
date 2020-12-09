CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_user_check_in_arc
(
    `id`         bigint COMMENT '',
    `user_id`    bigint COMMENT '',
    `count`      bigint COMMENT '',
    `last_date`  string COMMENT '',
    `per_count`  bigint COMMENT '',
    `full_count` bigint COMMENT ''
) comment '用户签到'
    PARTITIONED BY (pt STRING )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE;


INSERT overwrite table ods_fd_vb.ods_fd_user_check_in_arc PARTITION (pt = '${hiveconf:pt}')
select id,
       user_id,
       count,
       last_date,
       per_count,
       full_count
from tmp.tmp_fd_user_check_in_full;
