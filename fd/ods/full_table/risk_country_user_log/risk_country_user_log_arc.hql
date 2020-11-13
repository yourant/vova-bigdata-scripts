CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_risk_country_user_log_arc(
        `id` bigint COMMENT 'ID',
        `project` string COMMENT '组织',
        `ipcountry` string,
        `statecountry` string,
        `ip` string COMMENT 'ip',
        `time_zone` string COMMENT '时区',
        `sp_duid` string,
        `extension` string COMMENT '扩展',
        `create_time` string COMMENT '创建时间')
        COMMENT 'vbridal库屏蔽用户记录'
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_risk_country_user_log_arc PARTITION (dt='${hiveconf:dt}')
select
      id,
    project,
    ipcountry,
    statecountry,
    ip,
    time_zone,
    sp_duid,
    extension,
    create_time
from tmp.tmp_fd_risk_country_user_log_full;
