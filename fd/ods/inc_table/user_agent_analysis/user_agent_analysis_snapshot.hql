CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_user_agent_analysis (
`user_agent_id` bigint COMMENT '自增id',
`is_app` int COMMENT '是否是app',
`device_type` string COMMENT '设备类型',
`os_type` string COMMENT '操作系统类型',
`version` string COMMENT 'app版本号',
`device_id` string COMMENT '',
`uuid` string COMMENT 'uuid',
`project_name` string COMMENT '组织名',
`device_name` string COMMENT '设备详细名称',
`browser` string COMMENT '浏览器类型',
`idfa` string COMMENT 'idfa',
`idfv` string COMMENT 'idfv',
`imei` string COMMENT 'imei',
`android_id` string COMMENT 'android_id',
`ga_id` string COMMENT 'Google Advertising ID'
) COMMENT 'app message 推送信息log表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_user_agent_analysis
select `(dt)?+.+` 
from ods_fd_vb.ods_fd_user_agent_analysis_arc 
where dt = '${hiveconf:dt}';
