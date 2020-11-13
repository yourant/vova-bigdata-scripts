CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_app_event_log_message_push_arc(
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
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


INSERT overwrite table ods_fd_vb.ods_fd_app_event_log_message_push_arc PARTITION (dt='${hiveconf:dt}')
select
	id,
        event_type,
        event_name,
        event_value,
        /* timezone America/Los_Angeles in mysql ecshop database, convert to UTC */
        if(event_time != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(event_time, 'America/Los_Angeles'), "yyyy-MM-dd HH:mm:ss"), 0) as event_time,
        device_id,
        project,
        domain,
        platform,
        app_version,
        idfa,
        idfv,
        imei,
        android_id,
        ip,
        uid,
        language,
        country,
        currency,
        extra,
        system,
        bundle_id
from tmp.tmp_fd_app_event_log_message_push_full;
