CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_app_message_info
(
    `id` bigint COMMENT '主键 自增id',
    `status` bigint COMMENT '消息当前状态,-1:未启用,0:未发送,1:发送中,2:已发送',
    `project` string COMMENT '组织',
    `country_codes` string COMMENT '生效国家code多个用,隔开',
    `push_time` string COMMENT '发送时间',
    `push_time_end` string COMMENT '推送结束时间 - 用于重复推送时',
    `time_type` bigint COMMENT '时间类型，1：当地时间，2：洛杉矶时间，3：北京时间',
    `repeat` bigint COMMENT '推送频次，0单次，1每天，2每周，3每月',
    `event_type` bigint COMMENT '从10000开始',
    `user_active_day` bigint COMMENT '近n天活跃的用户',
    `platform` string COMMENT '发送平台多个用,隔开',
    `content` string COMMENT '消息主体',
    `title` string COMMENT '消息标题',
    `link_url` string COMMENT '跳转链接',
    `note` string COMMENT '备注',
    `create_time` string COMMENT '',
    `update_time` string COMMENT ''
 )comment '消息推送计划表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_app_message_info
select `(pt)?+.+` from ods_fd_vb.ods_fd_app_message_info_arc where pt = '${hiveconf:pt}';
