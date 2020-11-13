CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_user_agent_analysis(
  `user_agent_id` bigint,
  `is_app` bigint comment '是否是app',
  `device_type` string comment '设备类型',
  `os_type` string comment '操作系统类型',
  `version` string comment 'app版本号',
  `device_id` string comment 'device_id',
  `uuid` string comment 'uuid',
  `project_name` string comment '组织名',
  `device_name` string comment '设备详细名称',
  `browser` string comment '浏览器类型',
  `idfa` string comment 'idfa',
  `idfv` string comment 'idfv',
  `imei` string comment 'imei',
  `android_id` string comment 'android_id',
  `ga_id` string comment 'Google Advertising ID'
 )comment '用户ua信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_user_agent_analysis
select `(dt)?+.+` from ods_fd_vb.ods_fd_user_agent_analysis_arc where dt = '${hiveconf:dt}';
