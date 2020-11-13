CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_user_check_in_arc
(
    `id`         int COMMENT '',
    `user_id`    int COMMENT '',
    `count`      int COMMENT '',
    `last_date`  string COMMENT '',
    `per_count`  int COMMENT '',
    `full_count` int COMMENT ''
) comment '用户签到'
    PARTITIONED BY (dt STRING )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE
    TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_user_check_in_arc PARTITION (dt = '${hiveconf:dt}')
select id,
       user_id,
       count,
       last_date,
       per_count,
       full_count
from tmp.tmp_fd_user_check_in_full;
