CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_app_install_record_arc(
	`id` bigint COMMENT 'id',
`event_time` string COMMENT '安装时间',
`device_id` string COMMENT '设备id',
`notice_id` string COMMENT '推送id',
`user_id` bigint COMMENT 'user_id',
`project_name` string COMMENT '组织',
`platform` string COMMENT '平台',
`country_code` string COMMENT '国家',
`lang_code` string COMMENT '语言'
) COMMENT 'app安装记录表'
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_app_install_record_arc PARTITION (dt='${hiveconf:dt}')
select
 	id,
 	event_time,
        device_id,
        notice_id,
        user_id,
        project_name,
        platform,
        country_code,
        lang_code
from tmp.tmp_fd_app_install_record_full;









