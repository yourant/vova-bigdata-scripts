CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_app_install_record(
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_app_install_record 
select `(dt)?+.+`
from ods_fd_vb.ods_fd_app_install_record_arc
where dt = '${hiveconf:dt}';
