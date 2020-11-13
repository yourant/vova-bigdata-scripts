CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_app_event_log_message_push(
	`id` bigint COMMENT '',
    `event_type` string COMMENT '推送用户群组id',
    `event_name` string COMMENT '推送用户群组名',
    `event_value` bigint COMMENT 'app_message_push:id',
    `event_time` bigint COMMENT '时间-utc',
    `device_id` string COMMENT '设备id',
    `project` string COMMENT '组织',
    `domain` string COMMENT '',
    `platform` string COMMENT '平台',
    `app_version` string COMMENT 'app version',
    `idfa` string COMMENT 'idfa',
    `idfv` string COMMENT 'idfv',
    `imei` string COMMENT 'imei',
    `android_id` string COMMENT 'android_id',
    `ip` string COMMENT 'ip',
    `uid` bigint COMMENT 'uid',
    `language` string COMMENT '语言',
    `country` string COMMENT '国家',
    `currency` string COMMENT '货币',
    `extra` string COMMENT '',
    `system` string COMMENT '',
    `bundle_id` string COMMENT ''
) COMMENT 'app message 推送信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_app_event_log_message_push
select `(dt)?+.+`
from ods_fd_vb.ods_fd_app_event_log_message_push_arc
where dt = '${hiveconf:dt}';
