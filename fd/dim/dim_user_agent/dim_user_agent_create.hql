CREATE TABLE IF NOT EXISTS `dim.dim_fd_user_agent`(
  `user_agent_id` bigint COMMENT 'user agent id',
  `platform` string COMMENT '平台',
  `platform_type` string COMMENT '细分平台',
  `is_app` bigint COMMENT '是否APP',
  `device_type` string COMMENT '设备类型',
  `os_type` string COMMENT '操作系统类型',
  `version` string COMMENT 'APP版本号',
  `device_id` string COMMENT '设备id',
  `uuid` string COMMENT 'UUID',
  `device_name` string COMMENT '设备名称',
  `browser` string COMMENT '浏览器',
  `idfa` string COMMENT 'idfa',
  `idfv` string COMMENT 'idfv',
  `imei` string COMMENT 'imei',
  `android_id` string COMMENT 'android id',
  `ga_id` string COMMENT 'Google Advertising ID'
)
COMMENT '用户使用平台信息维表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;