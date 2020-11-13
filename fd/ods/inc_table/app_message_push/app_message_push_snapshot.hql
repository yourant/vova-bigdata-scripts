CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_app_message_push (
`id` bigint COMMENT '自增id',
`install_record_id` bigint COMMENT '用于关联app_install_record表',
`notice_id` string COMMENT '推送id',
`user_id` bigint COMMENT '用户id',
`event_type` bigint COMMENT '推送用户群组id',
`init_time` bigint COMMENT '记录插入时间',
`push_result` bigint COMMENT '发送结果标识',
`push_time` bigint COMMENT '推送时间'
) COMMENT 'app message 推送信息log表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_app_message_push
select `(dt)?+.+` 
from ods_fd_vb.ods_fd_app_message_push_arc
where dt = '${hiveconf:dt}';
